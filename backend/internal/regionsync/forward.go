package regionsync

import (
	"crypto/tls"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"
)

// NewPrimaryForwarder 把只能在主地区执行的请求反代到主地区。
//
// 只用于管理写、注册、第三方登录、支付回调等低频写入；模型请求、鉴权、扣费不经过这里。
// 管理员 JWT 两地通用（jwt.secret 相同，令牌版本由已同步的 email+password_hash 推导）。
// 客户端 IP 通过 X-Real-IP 传给主地区（主地区 nginx 需 set_real_ip_from 本机出口 IP），
// 会话绑定与审计日志因此与直连一致。
func NewPrimaryForwarder(target string, insecureSkipVerify bool, localRegion string, clientIP func(*http.Request) string) (http.Handler, error) {
	u, err := url.Parse(strings.TrimRight(strings.TrimSpace(target), "/"))
	if err != nil {
		return nil, err
	}
	base, _ := http.DefaultTransport.(*http.Transport)
	tr := base.Clone()
	tr.Proxy = nil
	tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: insecureSkipVerify} //nolint:gosec // 主地区为 IP 证书时由配置显式开启
	tr.DialContext = (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	tr.ResponseHeaderTimeout = 120 * time.Second

	proxy := &httputil.ReverseProxy{
		Transport: tr,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(u)
			pr.Out.Host = u.Host
			ip := ""
			if clientIP != nil {
				ip = clientIP(pr.In)
			}
			// 不透传客户端自带的转发头，只给主地区本机判定的真实 IP。
			pr.Out.Header.Del("X-Forwarded-For")
			pr.Out.Header.Del("X-Forwarded-Host")
			pr.Out.Header.Del("X-Forwarded-Proto")
			pr.Out.Header.Del("Forwarded")
			if ip != "" {
				pr.Out.Header.Set("X-Real-IP", ip)
				pr.Out.Header.Set("X-Forwarded-For", ip)
			}
			pr.Out.Header.Set("X-Region-Forwarded-By", localRegion)
		},
		ModifyResponse: func(resp *http.Response) error {
			// 本地 CORS / 安全头中间件已经写过一遍，去掉上游同名头避免重复。
			for k := range resp.Header {
				if strings.HasPrefix(strings.ToLower(k), "access-control-") {
					resp.Header.Del(k)
				}
			}
			resp.Header.Set("X-Region-Served-By", "primary")
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			slog.Warn("region_primary_forward_failed", "path", r.URL.Path, "method", r.Method, "error", sanitizeErr(err))
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"code":"REGION_PRIMARY_UNAVAILABLE","message":"管理主地区暂时不可达，写操作请稍后重试；只读页面不受影响"}`))
		},
	}
	return proxy, nil
}
