//go:build integration

package regionsync

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// bill 走真实的扣费仓储（与网关请求结束时同一路径）。
func bill(t *testing.T, n *node, requestID string, userID, keyID, accountID int64, cost float64) time.Duration {
	t.Helper()
	repo := repository.NewUsageBillingRepository(nil, n.db)
	start := time.Now()
	res, err := repo.Apply(context.Background(), &service.UsageBillingCommand{
		RequestID:   requestID,
		APIKeyID:    keyID,
		UserID:      userID,
		AccountID:   accountID,
		AccountType: service.AccountTypeAPIKey,
		Model:       "gpt-test",
		BalanceCost: cost,
	})
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.True(t, res.Applied)
	return elapsed
}

// ledger 模拟充值/退款/调账：业务事务里打上原因标签后改余额。
func ledger(t *testing.T, n *node, reason string, userID int64, amount string, recharge bool) {
	t.Helper()
	tx, err := n.db.Begin()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.Exec(`SELECT set_config('sub2api.region_sync_reason', $1, true)`, reason)
	require.NoError(t, err)
	q := `UPDATE users SET balance = balance + $1::numeric WHERE id = $2`
	if recharge {
		q = `UPDATE users SET balance = balance + $1::numeric, total_recharged = total_recharged + $1::numeric WHERE id = $2`
	}
	_, err = tx.Exec(q, amount, userID)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

func mustExec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	_, err := db.Exec(q, args...)
	require.NoError(t, err)
}

func queryString(t *testing.T, db *sql.DB, q string, args ...any) string {
	t.Helper()
	var s sql.NullString
	require.NoError(t, db.QueryRow(q, args...).Scan(&s))
	return s.String
}

func queryInt(t *testing.T, db *sql.DB, q string, args ...any) int64 {
	t.Helper()
	var v int64
	require.NoError(t, db.QueryRow(q, args...).Scan(&v))
	return v
}

func exchange(t *testing.T, a, b *node) {
	t.Helper()
	for i := 0; i < 3; i++ {
		require.NoError(t, a.pump(t))
		require.NoError(t, b.pump(t))
	}
	a.drain(t)
	b.drain(t)
}

// ---------------------------------------------------------------------------

func TestFeatureOffIsSingleNode(t *testing.T) {
	db, dsn := newDB(t, "off")
	cfg := config.RegionSyncConfig{Enabled: false}
	require.NoError(t, SyncLocalRow(context.Background(), db, cfg))
	svc := NewService(cfg, db, dsn, nil)

	_, _ = seedOne(t, db)
	bill(t, &node{db: db}, "off-1", 1001, 3001, 4001, 1.5)
	mustExec(t, db, `UPDATE users SET notes = 'x', username = 'y' WHERE id = 1001`)
	mustExec(t, db, `UPDATE api_keys SET name = 'renamed' WHERE id = 3001`)
	mustExec(t, db, `INSERT INTO usage_logs (user_id, api_key_id, account_id, model, request_id) VALUES (1001, 3001, 4001, 'm', 'r-off')`)
	require.EqualValues(t, 0, queryInt(t, db, `SELECT count(*) FROM region_sync_outbox`))
	require.EqualValues(t, 0, queryInt(t, db, `SELECT count(*) FROM region_sync_versions`))

	require.Nil(t, svc.Middleware(), "关闭时不挂守卫")
	r := gin.New()
	svc.RegisterRoutes(r)
	require.Empty(t, r.Routes(), "关闭时不注册内部接口")
}

// seedOne 单库基线（不经同步）。
func seedOne(t *testing.T, db *sql.DB) (int64, int64) {
	t.Helper()
	mustExec(t, db, `INSERT INTO users (id, email, password_hash, role, balance, concurrency, status, username, notes)
		VALUES (1001, 'u1@example.com', 'hash', 'user', 100, 5, 'active', 'u1', '')`)
	mustExec(t, db, `INSERT INTO groups (id, name, platform) VALUES (2001, 'g', 'openai')`)
	mustExec(t, db, `INSERT INTO api_keys (id, user_id, key, name, group_id, status) VALUES (3001, 1001, 'sk-test-1', 'k', 2001, 'active')`)
	mustExec(t, db, `INSERT INTO accounts (id, name, platform, type, credentials, extra) VALUES (4001, 'acc', 'openai', 'apikey', '{"api_key":"x"}', '{}')`)
	return 1001, 3001
}

func TestLinkDownRequestsStayLocal(t *testing.T) {
	my, gz := newPair(t)
	u, k, a := seedBaseline(t, my, gz)
	my.gate.mode.Store(gateDown)
	gz.gate.mode.Store(gateDown)

	var worst time.Duration
	for i := 0; i < 50; i++ {
		if d := bill(t, gz, fmt.Sprintf("gz-down-%d", i), u, k, a, 0.1); d > worst {
			worst = d
		}
		bill(t, my, fmt.Sprintf("my-down-%d", i), u, k, a, 0.2)
	}
	// 扣费事务里没有任何跨区调用：断链时请求照常完成，事件留在本地 outbox。
	require.Less(t, worst, 2*time.Second)
	require.Equal(t, "95.00000000", gz.balance(t, u))
	require.Equal(t, "90.00000000", my.balance(t, u))
	require.Error(t, gz.pump(t))
	require.Error(t, my.pump(t))
	require.EqualValues(t, 50, gz.pending(t))
	require.EqualValues(t, 50, my.pending(t))

	my.gate.mode.Store(gateOpen)
	gz.gate.mode.Store(gateOpen)
	exchange(t, my, gz)
	require.Equal(t, "85.00000000", gz.balance(t, u))
	require.Equal(t, "85.00000000", my.balance(t, u))
	require.Zero(t, gz.pending(t))
	require.Zero(t, my.pending(t))
}

func TestCrashAfterCommitResends(t *testing.T) {
	my, gz := newPair(t)
	u, k, a := seedBaseline(t, my, gz)

	// 扣费已提交，进程在发送前被杀：事件和业务变更在同一事务里，重启后照发。
	bill(t, gz, "crash-1", u, k, a, 7.25)
	gz.restart(t)
	require.EqualValues(t, 1, gz.pending(t))
	require.NoError(t, gz.pump(t))
	require.Equal(t, "92.75000000", my.balance(t, u))

	// 发送中途失败（对端不可达）再重启：未确认的事件不会被跳过。
	my.gate.mode.Store(gateDown)
	bill(t, gz, "crash-2", u, k, a, 1)
	bill(t, gz, "crash-3", u, k, a, 1)
	require.Error(t, gz.pump(t))
	gz.restart(t)
	my.gate.mode.Store(gateOpen)
	require.NoError(t, gz.pump(t))
	require.Equal(t, "90.75000000", my.balance(t, u))
	require.Zero(t, gz.pending(t))
}

func TestDuplicateAndLostAckDoNotDoubleCharge(t *testing.T) {
	my, gz := newPair(t)
	u, k, a := seedBaseline(t, my, gz)
	bill(t, gz, "dup-1", u, k, a, 10)

	// 对端已应用并提交，但确认丢在路上。
	my.gate.mode.Store(gateDropAck)
	require.Error(t, gz.pump(t))
	require.Equal(t, "90.00000000", my.balance(t, u))
	require.EqualValues(t, 1, gz.pending(t))

	// 重发：对端按 event_id 去重，结果为 duplicate，余额不变。
	my.gate.mode.Store(gateOpen)
	require.NoError(t, gz.pump(t))
	require.Equal(t, "90.00000000", my.balance(t, u))
	require.Equal(t, StatusDuplicate, queryString(t, gz.db, `SELECT ack_status FROM region_sync_outbox ORDER BY id DESC LIMIT 1`))

	// 再整批重放一遍已确认的事件（模拟对账误判重发）。
	mustExec(t, gz.db, `UPDATE region_sync_outbox SET acked_at = NULL, ack_status = NULL`)
	require.NoError(t, gz.pump(t))
	require.Equal(t, "90.00000000", my.balance(t, u))
	require.EqualValues(t, 1, queryInt(t, my.db, `SELECT count(*) FROM region_sync_inbox`))
}

// sendInOrder 把指定的 outbox 行逐条单独发送（模拟乱序到达）。
func (n *node) sendInOrder(t *testing.T, stream int, order []int) {
	t.Helper()
	rows, _, err := n.svc.fetchBatch(context.Background(), stream)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(rows), len(order))
	for _, i := range order {
		require.NoError(t, n.svc.sendBatch(context.Background(), []outboxRow{rows[i]}))
	}
}

func TestLateOldConfigCannotOverwrite(t *testing.T) {
	my, gz := newPair(t)
	u, _, a := seedBaseline(t, my, gz)

	mustExec(t, gz.db, `UPDATE users SET notes = 'v1' WHERE id = $1`, u)
	mustExec(t, gz.db, `UPDATE users SET notes = 'v2' WHERE id = $1`, u)
	gz.sendInOrder(t, StreamConfig, []int{1, 0})
	require.Equal(t, "v2", queryString(t, my.db, `SELECT notes FROM users WHERE id = $1`, u))
	require.Equal(t, StatusStale, queryString(t, gz.db, `SELECT ack_status FROM region_sync_outbox WHERE stream = 1 ORDER BY id LIMIT 1`))

	// 停用不会被晚到的旧事件复活（同一字段组按版本比较）。
	mustExec(t, gz.db, `UPDATE accounts SET schedulable = false WHERE id = $1`, a)
	mustExec(t, gz.db, `UPDATE accounts SET status = 'disabled' WHERE id = $1`, a)
	gz.sendInOrder(t, StreamConfig, []int{1, 0})
	require.Equal(t, "disabled", queryString(t, my.db, `SELECT status FROM accounts WHERE id = $1`, a))
	require.Zero(t, gz.pending(t))
}

func TestDeleteIsNotRevived(t *testing.T) {
	my, gz := newPair(t)
	u, k, a := seedBaseline(t, my, gz)

	// 硬删除：删除先到、旧的插入后到，墓碑挡住复活。
	mustExec(t, gz.db, `INSERT INTO account_groups (account_id, group_id, priority) VALUES ($1, 2001, 10)`, a)
	mustExec(t, gz.db, `DELETE FROM account_groups WHERE account_id = $1`, a)
	gz.sendInOrder(t, StreamConfig, []int{1, 0})
	require.EqualValues(t, 0, queryInt(t, my.db, `SELECT count(*) FROM account_groups WHERE account_id = $1`, a))

	// 软删除的 Key（用户在 MY 自助删除）不会被 GZ 之后的改名/启用复活。
	mustExec(t, my.db, `UPDATE api_keys SET deleted_at = NOW() WHERE id = $1`, k)
	time.Sleep(5 * time.Millisecond)
	mustExec(t, gz.db, `UPDATE api_keys SET name = 'renamed', status = 'active' WHERE id = $1`, k)
	exchange(t, my, gz)
	for _, n := range []*node{my, gz} {
		require.NotEmpty(t, queryString(t, n.db, `SELECT deleted_at::TEXT FROM api_keys WHERE id = $1`, k), n.id)
	}
	_ = u
}

func TestUserLocalChangesAndAdminFields(t *testing.T) {
	my, gz := newPair(t)
	u, k, _ := seedBaseline(t, my, gz)

	// 用户字段：最后写入者胜（GZ 先改、MY 后改 => MY 的值）。
	mustExec(t, gz.db, `UPDATE users SET username = 'from-gz' WHERE id = $1`, u)
	time.Sleep(5 * time.Millisecond)
	mustExec(t, my.db, `UPDATE users SET username = 'from-my' WHERE id = $1`, u)
	// 管理字段只认主地区：MY 本地改角色不外发，GZ 的备注照常下发。
	mustExec(t, my.db, `UPDATE users SET role = 'admin' WHERE id = $1`, u)
	mustExec(t, gz.db, `UPDATE users SET notes = 'vip' WHERE id = $1`, u)
	// 自助 Key 管理：MY 本地改名，异步到 GZ。
	mustExec(t, my.db, `UPDATE api_keys SET name = 'my-key' WHERE id = $1`, k)
	exchange(t, my, gz)

	for _, n := range []*node{my, gz} {
		require.Equal(t, "from-my", queryString(t, n.db, `SELECT username FROM users WHERE id = $1`, u), n.id)
		require.Equal(t, "vip", queryString(t, n.db, `SELECT notes FROM users WHERE id = $1`, u), n.id)
		require.Equal(t, "my-key", queryString(t, n.db, `SELECT name FROM api_keys WHERE id = $1`, k), n.id)
	}
	require.Equal(t, "user", queryString(t, gz.db, `SELECT role FROM users WHERE id = $1`, u))
	// 管理字段之后在 GZ 改一次，MY 上的本地越权值被覆盖回来。
	mustExec(t, gz.db, `UPDATE users SET role = 'user', concurrency = 7 WHERE id = $1`, u)
	exchange(t, my, gz)
	require.Equal(t, "user", queryString(t, my.db, `SELECT role FROM users WHERE id = $1`, u))
}

func TestConcurrentSpendConverges(t *testing.T) {
	my, gz := newPair(t)
	u, k, a := seedBaseline(t, my, gz)

	bill(t, gz, "gz-a", u, k, a, 3.5)
	bill(t, my, "my-a", u, k, a, 2.25)
	ledger(t, gz, "recharge", u, "50", true)
	bill(t, my, "my-b", u, k, a, 0.12345678)
	exchange(t, my, gz)

	want := "144.12654322"
	require.Equal(t, want, gz.balance(t, u))
	require.Equal(t, want, my.balance(t, u))
	require.Equal(t, "50.00000000", queryString(t, my.db, `SELECT total_recharged::TEXT FROM users WHERE id = $1`, u))
	c1, n1, err := BalanceChecksum(context.Background(), my.db)
	require.NoError(t, err)
	c2, n2, err := BalanceChecksum(context.Background(), gz.db)
	require.NoError(t, err)
	require.Equal(t, c1, c2)
	require.Equal(t, n1, n2)
}

func TestHoldCaptureReleaseRefund(t *testing.T) {
	my, gz := newPair(t)
	u, k, _ := seedBaseline(t, my, gz)
	repo := repository.NewUsageBillingRepository(nil, my.db)
	ctx := context.Background()
	hold := func(batch string, holdAmt, actual float64, op string) {
		cmd := &service.BatchImageBalanceHoldCommand{APIKeyID: k, UserID: u, BatchID: batch, HoldAmount: holdAmt, ActualAmount: actual}
		var err error
		switch op {
		case "reserve":
			cmd.RequestID = service.BatchImageHoldRequestID(batch)
			_, err = repo.ReserveBatchImageBalance(ctx, cmd)
		case "capture":
			cmd.RequestID = service.BatchImageCaptureRequestID(batch)
			_, err = repo.CaptureBatchImageBalance(ctx, cmd)
		case "release":
			cmd.RequestID = service.BatchImageReleaseRequestID(batch)
			_, err = repo.ReleaseBatchImageBalance(ctx, cmd)
		}
		require.NoError(t, err)
	}
	hold("A", 10, 0, "reserve")
	hold("A", 10, 6, "capture")
	hold("B", 5, 0, "reserve")
	hold("B", 5, 0, "release")
	ledger(t, gz, "refund", u, "3", false)
	ledger(t, gz, "admin_adjust", u, "-1.5", false)

	// 冻结/结算/释放与充值退款走高优先级流，不和消费混排。
	require.EqualValues(t, 0, queryInt(t, my.db, `SELECT count(*) FROM region_sync_outbox WHERE stream = 3`))
	exchange(t, my, gz)
	// 重复交换不改变结果。
	mustExec(t, my.db, `UPDATE region_sync_outbox SET acked_at = NULL`)
	mustExec(t, gz.db, `UPDATE region_sync_outbox SET acked_at = NULL`)
	exchange(t, my, gz)

	for _, n := range []*node{my, gz} {
		require.Equal(t, "95.50000000", n.balance(t, u), n.id)
		require.Equal(t, "0.00000000", n.frozen(t, u), n.id)
	}
}

func TestUsageLogsShipToAggregator(t *testing.T) {
	my, gz := newPair(t)
	u, k, a := seedBaseline(t, my, gz)
	// MY 是汇总地区：本地使用记录不外发。
	mustExec(t, my.db, `INSERT INTO usage_logs (user_id, api_key_id, account_id, model, request_id) VALUES ($1, $2, $3, 'm', 'r-my')`, u, k, a)
	for i := 0; i < 3; i++ {
		mustExec(t, gz.db, `INSERT INTO usage_logs (user_id, api_key_id, account_id, model, request_id) VALUES ($1, $2, $3, 'm', $4)`, u, k, a, fmt.Sprintf("r-gz-%d", i))
	}
	require.EqualValues(t, 0, my.pending(t))
	exchange(t, my, gz)
	require.EqualValues(t, 4, queryInt(t, my.db, `SELECT count(*) FROM usage_logs`))
	require.EqualValues(t, 3, queryInt(t, gz.db, `SELECT count(*) FROM usage_logs`))
	require.EqualValues(t, 3, queryInt(t, my.db, `SELECT count(*) FROM region_sync_usage_import WHERE origin_region = 'gz'`))

	// 外键未就绪（新 Key 的配置还没到）：先暂存，配置到达后自动补上。
	mustExec(t, gz.db, `INSERT INTO api_keys (id, user_id, key, name, status) VALUES (3002, $1, 'sk-test-2', 'k2', 'active')`, u)
	mustExec(t, gz.db, `INSERT INTO usage_logs (user_id, api_key_id, account_id, model, request_id) VALUES ($1, 3002, $2, 'm', 'r-new-key')`, u, a)
	rows, _, err := gz.svc.fetchBatch(context.Background(), StreamUsage)
	require.NoError(t, err)
	require.NoError(t, gz.svc.sendBatch(context.Background(), rows))
	require.EqualValues(t, 1, queryInt(t, my.db, `SELECT count(*) FROM region_sync_deferred WHERE kind = 'usage'`))
	require.NoError(t, gz.pump(t))
	_, err = my.svc.applier.RetryDeferred(context.Background(), 100)
	require.NoError(t, err)
	require.EqualValues(t, 0, queryInt(t, my.db, `SELECT count(*) FROM region_sync_deferred`))
	require.EqualValues(t, 1, queryInt(t, my.db, `SELECT count(*) FROM usage_logs WHERE request_id = 'r-new-key'`))
}

func TestReconcileRequeuesLostEvents(t *testing.T) {
	my, gz := newPair(t)
	u, k, a := seedBaseline(t, my, gz)
	bill(t, gz, "rec-1", u, k, a, 1)
	require.NoError(t, gz.pump(t))
	// 模拟一条事件在本地被标记确认、对端却从未收到。
	bill(t, gz, "rec-2", u, k, a, 2)
	mustExec(t, gz.db, `UPDATE region_sync_outbox SET acked_at = NOW(), ack_status = 'applied' WHERE acked_at IS NULL`)
	// 对账只看确认超过 30 秒的事件（避开刚确认、对端还在提交的窗口）。
	mustExec(t, gz.db, `UPDATE region_sync_outbox SET acked_at = acked_at - INTERVAL '1 minute'`)
	require.Equal(t, "99.00000000", my.balance(t, u))

	n, err := gz.svc.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NoError(t, gz.pump(t))
	require.Equal(t, "97.00000000", my.balance(t, u))
	n, err = gz.svc.Reconcile(context.Background())
	require.NoError(t, err)
	require.Zero(t, n)
}

func TestLongOutageCatchUp(t *testing.T) {
	if testing.Short() {
		t.Skip("long")
	}
	my, gz := newPair(t)
	u, k, a := seedBaseline(t, my, gz)
	my.gate.mode.Store(gateDown)

	const backlog = 1500
	base := make([]time.Duration, 0, backlog)
	for i := 0; i < backlog; i++ {
		base = append(base, bill(t, gz, fmt.Sprintf("out-%d", i), u, k, a, 0.01))
		mustExec(t, gz.db, `INSERT INTO usage_logs (user_id, api_key_id, account_id, model, request_id) VALUES ($1, $2, $3, 'm', $4)`, u, k, a, fmt.Sprintf("out-%d", i))
	}
	// 恢复前最后发生的配置与充值，必须先于积压的消费到达。
	mustExec(t, gz.db, `UPDATE api_keys SET status = 'disabled' WHERE id = $1`, k)
	ledger(t, gz, "recharge", u, "20", true)

	marker := queryInt(t, gz.db, `SELECT max(id) FROM region_sync_outbox`)
	backlogLeft := func() int64 {
		return queryInt(t, gz.db, `SELECT count(*) FROM region_sync_outbox WHERE acked_at IS NULL AND id <= $1`, marker)
	}

	my.gate.mode.Store(gateOpen)
	gz.svc.Start()
	t.Cleanup(gz.svc.Stop)

	during := make([]time.Duration, 0, 200)
	deadline := time.Now().Add(60 * time.Second)
	i := 0
	for backlogLeft() > 0 {
		require.True(t, time.Now().Before(deadline), "catch-up timed out, backlog=%d", backlogLeft())
		during = append(during, bill(t, gz, fmt.Sprintf("live-%d", i), u, k, a, 0.01))
		i++
		time.Sleep(5 * time.Millisecond)
	}
	gz.svc.Stop()
	require.NoError(t, gz.pump(t)) // 收尾：最后几条实时扣费

	p95 := func(d []time.Duration) time.Duration {
		if len(d) == 0 {
			return 0
		}
		s := append([]time.Duration(nil), d...)
		sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
		return s[len(s)*95/100]
	}
	t.Logf("billing p95: outage=%v catch-up=%v (samples %d)", p95(base), p95(during), len(during))
	require.Less(t, p95(during), 10*p95(base)+100*time.Millisecond)

	firstUsage := queryString(t, my.db, `SELECT min(received_at)::TEXT FROM region_sync_inbox WHERE stream = 3`)
	lastPriority := queryString(t, my.db, `SELECT max(received_at)::TEXT FROM region_sync_inbox WHERE stream IN (1, 2) AND origin_seq <= (SELECT max(origin_seq) FROM region_sync_inbox WHERE stream = 2)`)
	require.LessOrEqual(t, lastPriority, firstUsage, "配置/充值先于消费补发")

	require.Equal(t, gz.balance(t, u), my.balance(t, u))
	require.EqualValues(t, backlog, queryInt(t, my.db, `SELECT count(*) FROM usage_logs WHERE request_id LIKE 'out-%'`))
	require.Equal(t, "disabled", queryString(t, my.db, `SELECT status FROM api_keys WHERE id = $1`, k))
}

func TestBadSignatureRejected(t *testing.T) {
	my, _ := newPair(t)
	req, err := http.NewRequest(http.MethodPost, my.srv.URL+eventsPath, strings.NewReader(`{"origin":"gz","events":[]}`))
	require.NoError(t, err)
	req.Header.Set(HeaderOrigin, "gz")
	req.Header.Set(HeaderTimestamp, fmt.Sprint(time.Now().Unix()))
	req.Header.Set(HeaderSignature, "deadbeef")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

// MY 为管理入口：后台读本地、写转发到 GZ；模型请求与用户自助操作不跨区。
func TestNonPrimaryForwardsAdminWrites(t *testing.T) {
	var got []string
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.Method+" "+r.URL.Path+" ip="+r.Header.Get("X-Real-IP")+" by="+r.Header.Get("X-Region-Forwarded-By"))
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.WriteHeader(http.StatusOK)
	}))
	defer primary.Close()

	cfg := regionCfg("my", "gz")
	cfg.Peer.URL = primary.URL
	cfg.ForwardPrimaryWrites = true
	cfg.PrimaryForwardURL = primary.URL
	cfg.LocalAdminWritePrefixes = []string{"/api/v1/admin/ops"}
	svc := NewService(cfg, nil, "", nil)
	guard := svc.Middleware()
	require.NotNil(t, guard)

	r := gin.New()
	require.NoError(t, r.SetTrustedProxies(nil))
	r.Use(guard)
	local := func(c *gin.Context) { c.String(http.StatusOK, "local") }
	r.GET("/api/v1/admin/users", local)
	r.PUT("/api/v1/admin/users/:id", local)
	r.POST("/api/v1/admin/ops/retry", local)
	r.POST("/api/v1/keys", local)
	r.POST("/api/v1/auth/register", local)
	r.GET("/api/v1/auth/oauth/linuxdo/start", local)
	r.POST("/v1/responses", local)

	cases := []struct {
		method, path string
		local        bool
	}{
		{"GET", "/api/v1/admin/users", true},
		{"PUT", "/api/v1/admin/users/1", false},
		{"POST", "/api/v1/admin/ops/retry", true},
		{"POST", "/api/v1/keys", true},
		{"POST", "/api/v1/auth/register", false},
		{"GET", "/api/v1/auth/oauth/linuxdo/start", false},
		{"POST", "/v1/responses", true},
	}
	front := httptest.NewServer(r)
	defer front.Close()
	for _, c := range cases {
		req, err := http.NewRequest(c.method, front.URL+c.path, nil)
		require.NoError(t, err)
		req.Header.Set("X-Forwarded-For", "1.2.3.4")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode, c.path)
		require.Equal(t, c.local, string(body) == "local", c.method+" "+c.path)
		if !c.local {
			require.Empty(t, resp.Header.Get("Access-Control-Allow-Origin"), "上游 CORS 头被去掉")
			require.Equal(t, "primary", resp.Header.Get("X-Region-Served-By"))
		}
	}
	require.Len(t, got, 3)
	for _, g := range got {
		require.Contains(t, g, "ip=127.0.0.1 by=my", "只传本机判定的客户端 IP，不透传伪造的 XFF")
	}

	// 转发关闭：直接拒绝，提示主地区。
	cfg.ForwardPrimaryWrites = false
	svc = NewService(cfg, nil, "", nil)
	r = gin.New()
	r.Use(svc.Middleware())
	r.PUT("/api/v1/admin/users/:id", local)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("PUT", "/api/v1/admin/users/1", nil))
	require.Equal(t, http.StatusForbidden, w.Code)
	require.Contains(t, w.Body.String(), "REGION_PRIMARY_ONLY")

	// 主地区不挂守卫。
	gzCfg := regionCfg("gz", "my")
	gzCfg.Peer.URL = primary.URL
	require.Nil(t, NewService(gzCfg, nil, "", nil).Middleware())
}

// 只改配置即可换成 MY 为主：MY 的管理字段生效，GZ 的被忽略。
func TestMYPrimaryTopology(t *testing.T) {
	my, gz := newPair(t)
	for _, n := range []*node{my, gz} {
		n.cfg.PrimaryRegion = "my"
		n.cfg.UsageAggregationRegion = "my"
		require.NoError(t, SyncLocalRow(context.Background(), n.db, n.cfg))
		n.restart(t)
	}
	u, _, _ := seedBaseline(t, my, gz)
	mustExec(t, my.db, `UPDATE users SET notes = 'from-my-admin' WHERE id = $1`, u)
	mustExec(t, gz.db, `UPDATE users SET concurrency = 99 WHERE id = $1`, u)
	exchange(t, my, gz)
	require.Equal(t, "from-my-admin", queryString(t, gz.db, `SELECT notes FROM users WHERE id = $1`, u))
	require.EqualValues(t, 5, queryInt(t, my.db, `SELECT concurrency FROM users WHERE id = $1`, u))
	require.EqualValues(t, 0, queryInt(t, gz.db, `SELECT count(*) FROM region_sync_outbox WHERE stream = 1`))
}

// 注册表里的每张表、每个列都必须在真实迁移后的库里存在。
func TestRegistryMatchesSchema(t *testing.T) {
	db, _ := newDB(t, "schema")
	cols := func(table string) map[string]bool {
		rows, err := db.Query(`SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1`, table)
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()
		out := map[string]bool{}
		for rows.Next() {
			var c string
			require.NoError(t, rows.Scan(&c))
			out[c] = true
		}
		require.NotEmpty(t, out, "table %s missing", table)
		return out
	}
	for _, tbl := range ConfigTables {
		have := cols(tbl.Name)
		for _, pk := range tbl.PK {
			require.True(t, have[pk], "%s.%s", tbl.Name, pk)
		}
		for _, g := range tbl.Groups {
			for _, c := range g.Columns {
				require.True(t, have[columnName(c)], "%s.%s", tbl.Name, columnName(c))
			}
		}
		if tbl.StickyDeleteColumn != "" {
			require.True(t, have[tbl.StickyDeleteColumn], "%s.%s", tbl.Name, tbl.StickyDeleteColumn)
		}
	}
	for _, d := range DeltaTables {
		have := cols(d.Name)
		for _, c := range d.Columns {
			require.True(t, have[c.Name], "%s.%s", d.Name, c.Name)
			if c.WindowColumn != "" {
				require.True(t, have[c.WindowColumn], "%s.%s", d.Name, c.WindowColumn)
			}
		}
	}
	for _, name := range IDPartitionedTables {
		require.NotEmpty(t, queryString(t, db, `SELECT pg_get_serial_sequence($1, 'id')`, name), name)
	}
}
