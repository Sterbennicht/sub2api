package regionsync

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	maxWireBody    = 8 << 20
	maxDecodedBody = 32 << 20
)

// RegisterRoutes 挂内部同步接口。未开启多地区时不注册任何路由。
func (s *Service) RegisterRoutes(r gin.IRouter) {
	if !s.Enabled() {
		return
	}
	g := r.Group("/internal/region-sync/v1")
	g.POST("/events", s.handleEvents)
	g.POST("/reconcile", s.handleReconcileHTTP)
	g.GET("/status", s.handleStatus)
}

// readSigned 读取并校验签名请求，返回解压后的正文。失败时不回显任何细节。
func (s *Service) readSigned(c *gin.Context) ([]byte, bool) {
	wire, err := io.ReadAll(io.LimitReader(c.Request.Body, maxWireBody+1))
	if err != nil || len(wire) > maxWireBody {
		c.AbortWithStatus(http.StatusRequestEntityTooLarge)
		return nil, false
	}
	maxSkew := time.Duration(s.cfg.MaxClockSkewSeconds) * time.Second
	if err := verifyRequest(s.cfg.Peer.SharedSecret, s.cfg.Peer.RegionID,
		c.GetHeader(HeaderOrigin), c.Request.Method, c.Request.URL.Path,
		c.GetHeader(HeaderTimestamp), c.GetHeader(HeaderSignature), wire, maxSkew, s.now()); err != nil {
		slog.Warn("region_sync_auth_rejected", "remote", c.ClientIP(), "path", c.Request.URL.Path)
		c.AbortWithStatus(http.StatusUnauthorized)
		return nil, false
	}
	body := wire
	if strings.EqualFold(c.GetHeader("Content-Encoding"), "gzip") {
		zr, err := gzip.NewReader(bytes.NewReader(wire))
		if err != nil {
			c.AbortWithStatus(http.StatusBadRequest)
			return nil, false
		}
		body, err = io.ReadAll(io.LimitReader(zr, maxDecodedBody+1))
		if err != nil || len(body) > maxDecodedBody {
			c.AbortWithStatus(http.StatusRequestEntityTooLarge)
			return nil, false
		}
	}
	return body, true
}

func (s *Service) handleEvents(c *gin.Context) {
	body, ok := s.readSigned(c)
	if !ok {
		return
	}
	var batch Batch
	if err := json.Unmarshal(body, &batch); err != nil {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}
	if batch.Origin != s.cfg.Peer.RegionID {
		c.AbortWithStatus(http.StatusForbidden)
		return
	}
	if batch.SchemaVersion > SchemaVersion {
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{"supported_schema_version": SchemaVersion})
		return
	}
	for _, ev := range batch.Events {
		if ev.SchemaVersion > SchemaVersion {
			c.AbortWithStatusJSON(http.StatusConflict, gin.H{"supported_schema_version": SchemaVersion})
			return
		}
	}
	results, err := s.applier.ApplyBatch(c.Request.Context(), &batch)
	if err != nil {
		// 可重试错误：整批回滚，发送端整批重发；去重保证不会重复应用。
		slog.Warn("region_sync_apply_batch_failed", "events", len(batch.Events), "error", errorClass(err))
		c.AbortWithStatus(http.StatusServiceUnavailable)
		return
	}
	s.poke(s.invWake)
	for _, r := range results {
		if r.Status == StatusRejected {
			slog.Error("region_sync_event_rejected", "event_id", r.ID, "reason", r.Error)
		}
	}
	c.JSON(http.StatusOK, gin.H{"results": results})
}

func (s *Service) handleReconcileHTTP(c *gin.Context) {
	body, ok := s.readSigned(c)
	if !ok {
		return
	}
	var req ReconcileRequest
	if err := json.Unmarshal(body, &req); err != nil || req.Origin != s.cfg.Peer.RegionID {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}
	resp, err := handleReconcile(c.Request.Context(), s.db, &req)
	if err != nil {
		c.AbortWithStatus(http.StatusServiceUnavailable)
		return
	}
	c.JSON(http.StatusOK, resp)
}

// handleStatus 只接受带签名的请求。服务通常在 nginx 后面，来源地址恒为 127.0.0.1，
// 不能按回环地址放行；本机查看用 sub2api region-sync status。
func (s *Service) handleStatus(c *gin.Context) {
	if _, ok := s.readSigned(c); !ok {
		return
	}
	st, err := s.Status(c.Request.Context())
	if err != nil {
		c.AbortWithStatus(http.StatusServiceUnavailable)
		return
	}
	c.JSON(http.StatusOK, st)
}

// ---------------------------------------------------------------------------
// 非主地区的写保护：管理后台只在主地区；用户中心只放行本地可完成的自助操作。
// 依据的是服务端路由（gin FullPath），不依赖前端隐藏按钮。

type routeRule struct {
	method string
	path   string // gin FullPath；以 /* 结尾表示前缀
}

// NonPrimaryWritable 非主地区允许的写操作：登录、个人设置、自助 Key 管理等。
// 这些写入落本地库，由触发器捕获后异步同步到主地区，不依赖 MY 在线。
var NonPrimaryWritable = []routeRule{
	{"POST", "/api/v1/auth/login"},
	{"POST", "/api/v1/auth/login/2fa"},
	{"POST", "/api/v1/auth/passkey/login/begin"},
	{"POST", "/api/v1/auth/passkey/login/finish"},
	{"POST", "/api/v1/auth/refresh"},
	{"POST", "/api/v1/auth/logout"},
	{"POST", "/api/v1/auth/send-verify-code"},
	{"POST", "/api/v1/auth/forgot-password"},
	{"POST", "/api/v1/auth/reset-password"},
	{"PUT", "/api/v1/user"},
	{"PUT", "/api/v1/user/password"},
	{"*", "/api/v1/user/notify-email/*"},
	{"*", "/api/v1/user/totp/*"},
	{"*", "/api/v1/user/passkeys/*"},
	{"*", "/api/v1/user/passkeys"},
	{"POST", "/api/v1/keys"},
	{"PUT", "/api/v1/keys/:id"},
	{"DELETE", "/api/v1/keys/:id"},
	{"POST", "/api/v1/announcements/:id/read"},
	{"POST", "/api/v1/usage/dashboard/api-keys-usage"},
}

// NonPrimaryPrimaryOnlyPrefixes 非主地区连 GET 也交给主地区处理的入口：第三方登录的发起与回调
// （state 存在主地区 Redis，会建号/绑号），以及支付回调。
var NonPrimaryPrimaryOnlyPrefixes = []string{
	"/api/v1/auth/oauth",
	"/api/v1/payment/webhook",
}

const adminPrefix = "/api/v1/admin"

func hasPathPrefix(path, prefix string) bool {
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}

// GuardOptions 非主地区写保护的配置。
type GuardOptions struct {
	Enabled       bool
	RegionID      string
	PrimaryRegion string
	PrimaryURL    string
	// Forward 非 nil 时，「只能在主地区执行」的请求转发过去，否则 403。
	Forward http.Handler
	// LocalAdminWritePrefixes 本地执行的管理写（只影响本机的运维类）。
	LocalAdminWritePrefixes []string
}

// GuardDecision 守卫对一个请求的处理方式。
type GuardDecision int

const (
	GuardLocal GuardDecision = iota
	GuardPrimary
)

// Decide 判定请求在本地处理还是交给主地区。依据服务端路由，不依赖前端。
//
//   - 模型网关等 /api/v1 以外的路径：本地。
//   - 管理后台：GET 读本地（允许落后于主地区几秒）；运维类写本地；其余写交给主地区。
//   - 第三方登录、支付回调：交给主地区。
//   - 用户中心：只读与 NonPrimaryWritable（登录、个人设置、自助 Key 等）本地，其余写交给主地区。
func (o GuardOptions) Decide(method, fullPath string) GuardDecision {
	if !strings.HasPrefix(fullPath, "/api/v1/") {
		return GuardLocal
	}
	for _, p := range NonPrimaryPrimaryOnlyPrefixes {
		if hasPathPrefix(fullPath, p) {
			return GuardPrimary
		}
	}
	readOnly := method == http.MethodGet || method == http.MethodHead || method == http.MethodOptions
	if hasPathPrefix(fullPath, adminPrefix) {
		if readOnly {
			return GuardLocal
		}
		for _, p := range o.LocalAdminWritePrefixes {
			if hasPathPrefix(fullPath, strings.TrimRight(p, "/")) {
				return GuardLocal
			}
		}
		return GuardPrimary
	}
	if readOnly || matchRule(NonPrimaryWritable, method, fullPath) {
		return GuardLocal
	}
	return GuardPrimary
}

// WriteGuard 返回中间件；主地区和单节点返回 nil（不挂任何逻辑）。
func WriteGuard(o GuardOptions) gin.HandlerFunc {
	if !o.Enabled || o.RegionID == o.PrimaryRegion {
		return nil
	}
	return func(c *gin.Context) {
		full := c.FullPath()
		if full == "" {
			full = c.Request.URL.Path
		}
		if o.Decide(c.Request.Method, full) == GuardLocal {
			c.Next()
			return
		}
		if o.Forward != nil {
			req := c.Request.WithContext(context.WithValue(c.Request.Context(), clientIPKey{}, c.ClientIP()))
			o.Forward.ServeHTTP(c.Writer, req)
			c.Abort()
			return
		}
		denyPrimaryOnly(c, o.PrimaryRegion, o.PrimaryURL)
	}
}

func denyPrimaryOnly(c *gin.Context, primaryRegion, primaryURL string) {
	body := gin.H{
		"code":           "REGION_PRIMARY_ONLY",
		"message":        "该操作只能在管理主地区进行",
		"primary_region": primaryRegion,
	}
	if primaryURL != "" {
		body["primary_url"] = primaryURL
	}
	c.AbortWithStatusJSON(http.StatusForbidden, body)
}

func matchRule(rules []routeRule, method, fullPath string) bool {
	for _, r := range rules {
		if r.method != "*" && r.method != method {
			continue
		}
		if strings.HasSuffix(r.path, "/*") {
			if strings.HasPrefix(fullPath, strings.TrimSuffix(r.path, "*")) {
				return true
			}
			continue
		}
		if r.path == fullPath {
			return true
		}
	}
	return false
}

type clientIPKey struct{}

// ForwardedClientIP 取守卫按本地可信代理规则判定的客户端 IP。
func ForwardedClientIP(r *http.Request) string {
	ip, _ := r.Context().Value(clientIPKey{}).(string)
	return ip
}
