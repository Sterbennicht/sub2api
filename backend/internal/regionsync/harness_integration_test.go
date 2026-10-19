//go:build integration

package regionsync

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/region"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// 生产库为 PostgreSQL 16。
const testPostgresImage = "postgres:16-alpine"

var (
	adminDSN    string // 连 postgres 库，用来按模板建库
	dbSeq       atomic.Int64
	pgContainer *tcpostgres.PostgresContainer
)

const testSecret = "test-shared-secret-0123456789abcdef-0123456789"

func TestMain(m *testing.M) {
	ctx := context.Background()
	if err := exec.CommandContext(ctx, "docker", "info").Run(); err != nil {
		if os.Getenv("CI") != "" {
			log.Printf("docker is not available (CI=true)")
			os.Exit(1)
		}
		log.Printf("docker is not available; skipping regionsync integration tests")
		os.Exit(0)
	}
	pg, err := tcpostgres.Run(ctx, testPostgresImage,
		tcpostgres.WithDatabase("rs_template"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		log.Printf("start postgres: %v", err)
		os.Exit(1)
	}
	defer func() { _ = pg.Terminate(ctx) }()
	pgContainer = pg
	tmplDSN, err := pg.ConnectionString(ctx, "sslmode=disable", "TimeZone=UTC")
	if err != nil {
		log.Printf("dsn: %v", err)
		os.Exit(1)
	}
	tmpl, err := openRetry(tmplDSN)
	if err != nil {
		log.Printf("open: %v", err)
		os.Exit(1)
	}
	if err := repository.ApplyMigrations(ctx, tmpl); err != nil {
		log.Printf("migrations: %v", err)
		os.Exit(1)
	}
	_ = tmpl.Close()
	adminDSN = strings.Replace(tmplDSN, "/rs_template?", "/postgres?", 1)
	gin.SetMode(gin.TestMode)
	code := m.Run()
	region.Reset()
	os.Exit(code)
}

func openRetry(dsn string) (*sql.DB, error) {
	deadline := time.Now().Add(30 * time.Second)
	for {
		db, err := sql.Open("postgres", dsn)
		if err == nil {
			if err = db.Ping(); err == nil {
				return db, nil
			}
			_ = db.Close()
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// newDB 从已迁移的模板库复制一个独立库（相当于一个地区的本地 PostgreSQL）。
func newDB(t *testing.T, label string) (*sql.DB, string) {
	t.Helper()
	admin, err := sql.Open("postgres", adminDSN)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()
	name := fmt.Sprintf("rs_%s_%d", label, dbSeq.Add(1))
	_, err = admin.Exec(`CREATE DATABASE ` + name + ` TEMPLATE rs_template`)
	require.NoError(t, err)
	dsn := strings.Replace(adminDSN, "/postgres?", "/"+name+"?", 1)
	db, err := openRetry(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db, dsn
}

// gate 可切换的对端入口：正常、断链、应用后丢确认。
type gate struct {
	mode    atomic.Int32 // 0 正常, 1 断链, 2 丢确认
	handler atomic.Value
	hits    atomic.Int64
}

const (
	gateOpen = iota
	gateDown
	gateDropAck
)

func (g *gate) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.hits.Add(1)
	switch g.mode.Load() {
	case gateDown:
		hj, ok := w.(http.Hijacker)
		if ok {
			if conn, _, err := hj.Hijack(); err == nil {
				_ = conn.Close()
				return
			}
		}
		w.WriteHeader(http.StatusBadGateway)
		return
	case gateDropAck:
		// 先让接收端完整处理并提交，再把响应丢掉：模拟「对端已落库，确认在路上丢了」。
		rec := httptest.NewRecorder()
		g.handler.Load().(http.Handler).ServeHTTP(rec, r)
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				_ = conn.Close()
				return
			}
		}
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	g.handler.Load().(http.Handler).ServeHTTP(w, r)
}

type node struct {
	id   string
	db   *sql.DB
	dsn  string
	cfg  config.RegionSyncConfig
	svc  *Service
	gate *gate
	srv  *httptest.Server
}

func regionCfg(id, peer string) config.RegionSyncConfig {
	c := config.RegionSyncConfig{
		Enabled:                true,
		RegionID:               id,
		PrimaryRegion:          "gz",
		UsageAggregationRegion: "my",
		SenderEnabled:          true,
		Peer:                   config.RegionSyncPeerConfig{RegionID: peer, SharedSecret: testSecret, TimeoutSeconds: 5},
		ConfigScanIntervalMS:   1000,
		LedgerScanIntervalMS:   1000,
		UsageFlushIntervalMS:   2000,
		BatchMaxEvents:         500,
		BatchMaxBytes:          512 * 1024,
		RetryMinMS:             50,
		RetryMaxMS:             200,
		CatchupMaxBatchesPerS:  0,
		ReconcileIntervalS:     300,
		ReconcileWindowHours:   48,
		OutboxRetentionHours:   168,
		InboxRetentionHours:    720,
		MaxClockSkewSeconds:    300,
		BacklogAlertAgeSeconds: 300,
	}
	return c
}

// newPair 建 MY、GZ 两个地区：各自本地库 + 同步服务 + 对外入口（经 gate，可模拟断链）。
// 服务不启动后台循环，测试用 pump 手动推进，结果确定。
func newPair(t *testing.T) (my, gz *node) {
	t.Helper()
	my = &node{id: "my", gate: &gate{}}
	gz = &node{id: "gz", gate: &gate{}}
	for _, n := range []*node{my, gz} {
		n.db, n.dsn = newDB(t, n.id)
		n.srv = httptest.NewServer(n.gate)
		t.Cleanup(n.srv.Close)
	}
	my.cfg = regionCfg("my", "gz")
	my.cfg.Peer.URL = gz.srv.URL
	gz.cfg = regionCfg("gz", "my")
	gz.cfg.Peer.URL = my.srv.URL
	for _, n := range []*node{my, gz} {
		n.svc = NewService(n.cfg, n.db, n.dsn, nil)
		require.NoError(t, SyncLocalRow(context.Background(), n.db, n.cfg))
		r := gin.New()
		n.svc.RegisterRoutes(r)
		n.gate.handler.Store(http.Handler(r))
	}
	// 进程内两个地区共享 region 包状态，这里只需要「已启用」让扣费事务打标。
	region.Set(region.Identity{Enabled: true, RegionID: "gz", PrimaryRegion: "gz"})
	t.Cleanup(region.Reset)
	return my, gz
}

// restart 模拟进程重启：丢掉内存状态，只保留本地库。
func (n *node) restart(t *testing.T) {
	n.svc = NewService(n.cfg, n.db, n.dsn, nil)
	r := gin.New()
	n.svc.RegisterRoutes(r)
	n.gate.handler.Store(http.Handler(r))
}

// pump 按发送端的优先级把 outbox 推完；返回第一个发送错误。
func (n *node) pump(t *testing.T) error {
	t.Helper()
	ctx := context.Background()
	for _, stream := range []int{StreamConfig, StreamLedger, StreamUsage} {
		for i := 0; i < 1000; i++ {
			rows, _, err := n.svc.fetchBatch(ctx, stream)
			require.NoError(t, err)
			if len(rows) == 0 {
				break
			}
			if err := n.svc.sendBatch(ctx, rows); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *node) drain(t *testing.T) {
	_, err := DrainInvalidations(context.Background(), n.db, nil, 1000)
	require.NoError(t, err)
}

func (n *node) pending(t *testing.T) int64 {
	var c int64
	require.NoError(t, n.db.QueryRow(`SELECT count(*) FROM region_sync_outbox WHERE acked_at IS NULL`).Scan(&c))
	return c
}

func (n *node) balance(t *testing.T, userID int64) string {
	var b string
	require.NoError(t, n.db.QueryRow(`SELECT balance::TEXT FROM users WHERE id = $1`, userID).Scan(&b))
	return b
}

func (n *node) frozen(t *testing.T, userID int64) string {
	var b string
	require.NoError(t, n.db.QueryRow(`SELECT frozen_balance::TEXT FROM users WHERE id = $1`, userID).Scan(&b))
	return b
}

// seedBaseline 在两地写入相同的初始数据（模拟初始化基线），不产生事件。
func seedBaseline(t *testing.T, nodes ...*node) (userID, keyID, accountID int64) {
	t.Helper()
	for _, n := range nodes {
		tx, err := n.db.Begin()
		require.NoError(t, err)
		_, err = tx.Exec(`SELECT set_config('sub2api.region_sync_applying', 'on', true)`)
		require.NoError(t, err)
		require.NoError(t, tx.QueryRow(`
			INSERT INTO users (id, email, password_hash, role, balance, concurrency, status, username, notes)
			VALUES (1001, 'u1@example.com', 'hash', 'user', 100, 5, 'active', 'u1', '') RETURNING id`).Scan(&userID))
		require.NoError(t, tx.QueryRow(`
			INSERT INTO groups (id, name, platform) VALUES (2001, 'g', 'openai') RETURNING id`).Scan(new(int64)))
		require.NoError(t, tx.QueryRow(`
			INSERT INTO api_keys (id, user_id, key, name, group_id, status) VALUES (3001, 1001, 'sk-test-1', 'k', 2001, 'active') RETURNING id`).Scan(&keyID))
		require.NoError(t, tx.QueryRow(`
			INSERT INTO accounts (id, name, platform, type, credentials, extra) VALUES (4001, 'acc', 'openai', 'apikey', '{"api_key":"x"}', '{}') RETURNING id`).Scan(&accountID))
		require.NoError(t, tx.Commit())
	}
	return
}
