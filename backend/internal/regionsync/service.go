// Package regionsync 实现 MY/GZ 异地多活的后台事件同步。
//
// 请求路径只写本地库：业务事务里的触发器把待同步事件写进 region_sync_outbox，
// 本包的 worker 在事务提交后读取并推送给对端；对端在一个事务里去重并应用，
// 提交后才确认。链路断开时事件留在本地 outbox，恢复后按优先级补发。
package regionsync

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/region"
	"github.com/lib/pq"
)

const (
	eventsPath    = "/internal/region-sync/v1/events"
	reconcilePath = "/internal/region-sync/v1/reconcile"
	statusPath    = "/internal/region-sync/v1/status"
	wakeChannel   = "region_sync_wake"
)

func int64Array(v []int64) any { return pq.Array(v) }

// Service 地区同步的发送端、接收端与维护任务。
type Service struct {
	cfg     config.RegionSyncConfig
	db      *sql.DB
	dsn     string
	applier *Applier
	inv     CacheInvalidator
	client  *http.Client

	wake       chan struct{}
	invWake    chan struct{}
	stopCh     chan struct{}
	wg         sync.WaitGroup
	startOnce  sync.Once
	stopOnce   sync.Once
	reconcileM sync.Mutex

	failures      atomic.Int64
	lastSuccessNS atomic.Int64
	lastErrorNS   atomic.Int64
	lastError     atomic.Pointer[string]
	rejectedTotal atomic.Int64

	now func() time.Time
}

// NewService 创建同步服务。cfg.Enabled=false 时返回的服务只负责把本地开关写成关闭。
func NewService(cfg config.RegionSyncConfig, db *sql.DB, dsn string, inv CacheInvalidator) *Service {
	s := &Service{
		cfg:     cfg,
		db:      db,
		dsn:     dsn,
		inv:     inv,
		wake:    make(chan struct{}, 1),
		invWake: make(chan struct{}, 1),
		stopCh:  make(chan struct{}),
		now:     time.Now,
	}
	if cfg.Enabled {
		s.applier = NewApplier(db, cfg.RegionID, cfg.PrimaryRegion, cfg.LocalSettingKeys)
		s.applier.usageAggregator = cfg.IsUsageAggregator()
		s.client = newPeerClient(cfg.Peer)
	}
	return s
}

func newPeerClient(p config.RegionSyncPeerConfig) *http.Client {
	base, _ := http.DefaultTransport.(*http.Transport)
	tr := base.Clone()
	tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: p.InsecureSkipVerify} //nolint:gosec // 对端为 IP 证书时由配置显式开启；请求另有 HMAC 鉴权
	if p.ProxyURL != "" {
		if u, err := url.Parse(p.ProxyURL); err == nil {
			tr.Proxy = http.ProxyURL(u)
		}
	} else {
		tr.Proxy = nil
	}
	tr.MaxIdleConnsPerHost = 4
	timeout := time.Duration(p.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	return &http.Client{Transport: tr, Timeout: timeout}
}

// Enabled 是否开启多地区。
func (s *Service) Enabled() bool { return s != nil && s.cfg.Enabled }

// Config 返回配置副本。
func (s *Service) Config() config.RegionSyncConfig { return s.cfg }

// PublishIdentity 把地区身份写进进程全局（调度分池、OAuth 刷新 owner、代理覆盖）。
func PublishIdentity(cfg config.RegionSyncConfig) {
	if !cfg.Enabled {
		region.Reset()
		return
	}
	overrides := map[int64]string{}
	for k, v := range cfg.ProxyURLOverrides {
		var id int64
		if _, err := fmt.Sscan(strings.TrimSpace(k), &id); err == nil && id > 0 && strings.TrimSpace(v) != "" {
			overrides[id] = strings.TrimSpace(v)
		}
	}
	region.Set(region.Identity{
		Enabled:           true,
		RegionID:          cfg.RegionID,
		PrimaryRegion:     cfg.PrimaryRegion,
		DefaultOAuthOwner: cfg.DefaultOAuthRefreshOwner,
		PrimaryPublicURL:  cfg.PrimaryPublicURL,
		ProxyURLOverrides: overrides,
	})
}

// SyncLocalRow 把本地开关写进 region_sync_local，触发器据此决定是否捕获。
// 关闭时把已有行置为 enabled=false，单节点行为保持不变。只访问本地库。
func SyncLocalRow(ctx context.Context, db *sql.DB, cfg config.RegionSyncConfig) error {
	if !cfg.Enabled {
		_, err := db.ExecContext(ctx, `UPDATE region_sync_local SET enabled = FALSE, updated_at = NOW() WHERE enabled`)
		return err
	}
	primary := cfg.RegionID == cfg.PrimaryRegion
	_, err := db.ExecContext(ctx, `
		INSERT INTO region_sync_local (id, region_id, is_primary, enabled, ship_usage, updated_at)
		VALUES (1, $1, $2, TRUE, $3, NOW())
		ON CONFLICT (id) DO UPDATE SET region_id = EXCLUDED.region_id, is_primary = EXCLUDED.is_primary,
			enabled = TRUE, ship_usage = EXCLUDED.ship_usage, updated_at = NOW()`,
		cfg.RegionID, primary, !cfg.IsUsageAggregator())
	return err
}

// Start 启动后台任务。不等待对端、不阻塞启动就绪。
func (s *Service) Start() {
	if s == nil {
		return
	}
	s.startOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := SyncLocalRow(ctx, s.db, s.cfg); err != nil {
			slog.Error("region_sync_local_row_failed", "error", err)
		}
		cancel()
		PublishIdentity(s.cfg)
		if !s.cfg.Enabled {
			return
		}
		s.warnUnclassifiedColumns()
		s.wg.Add(3)
		go s.listenLoop()
		go s.senderLoop()
		go s.maintenanceLoop()
		slog.Info("region_sync_started", "region", s.cfg.RegionID, "primary", s.cfg.PrimaryRegion, "peer", s.cfg.Peer.RegionID, "sender_enabled", s.cfg.SenderEnabled)
	})
}

// Stop 停止后台任务。未发出的事件留在 outbox，重启后继续。
func (s *Service) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		close(s.stopCh)
		s.wg.Wait()
	})
}

func (s *Service) poke(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// listenLoop LISTEN 事务提交后的 NOTIFY，配置/充值变更立即唤醒发送；失败时退化为定时扫描。
func (s *Service) listenLoop() {
	defer s.wg.Done()
	if s.dsn == "" {
		return
	}
	listener := pq.NewListener(s.dsn, time.Second, 30*time.Second, func(ev pq.ListenerEventType, err error) {
		if err != nil {
			slog.Warn("region_sync_listener_event", "event", int(ev), "error", err)
		}
	})
	defer func() { _ = listener.Close() }()
	if err := listener.Listen(wakeChannel); err != nil {
		slog.Warn("region_sync_listen_failed", "error", err)
	}
	ping := time.NewTicker(90 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case n := <-listener.Notify:
			if n != nil {
				s.poke(s.wake)
			}
		case <-ping.C:
			_ = listener.Ping()
		}
	}
}

func (s *Service) sleep(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-s.stopCh:
		return false
	case <-t.C:
		return true
	}
}

// backoff 1、2、4、8、16、30 秒并加抖动（区间 [base/2, base]）。
func (s *Service) backoff(failures int64) time.Duration {
	minD := time.Duration(s.cfg.RetryMinMS) * time.Millisecond
	maxD := time.Duration(s.cfg.RetryMaxMS) * time.Millisecond
	d := minD
	for i := int64(1); i < failures && d < maxD; i++ {
		d *= 2
	}
	if d > maxD {
		d = maxD
	}
	half := d / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}

type outboxRow struct {
	id      int64
	ev      Event
	payload []byte
}

// fetchBatch 取一批未确认事件；more 表示因条数或字节上限截断、后面还有。
func (s *Service) fetchBatch(ctx context.Context, stream int) (_ []outboxRow, more bool, _ error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, event_id, origin_region, schema_version, stream, entity_type, entity_key, payload
		FROM region_sync_outbox
		WHERE acked_at IS NULL AND stream = $1
		ORDER BY id
		LIMIT $2`, stream, s.cfg.BatchMaxEvents)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = rows.Close() }()
	var out []outboxRow
	size := 0
	for rows.Next() {
		var r outboxRow
		if err := rows.Scan(&r.id, &r.ev.ID, &r.ev.Origin, &r.ev.SchemaVersion, &r.ev.Stream, &r.ev.EntityType, &r.ev.EntityKey, &r.payload); err != nil {
			return nil, false, err
		}
		size += len(r.payload) + 200
		if len(out) > 0 && size > s.cfg.BatchMaxBytes {
			return out, true, nil
		}
		r.ev.Seq = r.id
		r.ev.Payload = r.payload
		out = append(out, r)
	}
	return out, len(out) >= s.cfg.BatchMaxEvents, rows.Err()
}

var errSchemaMismatch = errors.New("peer rejected schema version")

func (s *Service) post(ctx context.Context, path string, body any, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(raw); err != nil {
		return err
	}
	if err := gz.Close(); err != nil {
		return err
	}
	wire := buf.Bytes()
	endpoint := strings.TrimRight(s.cfg.Peer.URL, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(wire))
	if err != nil {
		return err
	}
	ts := s.now().Unix()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set(HeaderOrigin, s.cfg.RegionID)
	req.Header.Set(HeaderTimestamp, fmt.Sprint(ts))
	req.Header.Set(HeaderSignature, signRequest(s.cfg.Peer.SharedSecret, s.cfg.RegionID, http.MethodPost, path, ts, wire))
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if resp.StatusCode == http.StatusConflict {
		return errSchemaMismatch
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("peer http %d", resp.StatusCode)
	}
	return json.Unmarshal(data, out)
}

// sendBatch 推送一批并按对端结果确认。只有对端提交后返回的结果才会让事件出队。
func (s *Service) sendBatch(ctx context.Context, rows []outboxRow) error {
	batch := Batch{Origin: s.cfg.RegionID, SchemaVersion: SchemaVersion, Events: make([]Event, len(rows))}
	for i := range rows {
		batch.Events[i] = rows[i].ev
	}
	var resp struct {
		Results []Result `json:"results"`
	}
	sendCtx, cancel := context.WithTimeout(ctx, s.client.Timeout+5*time.Second)
	defer cancel()
	err := s.post(sendCtx, eventsPath, batch, &resp)
	ids := make([]int64, len(rows))
	for i := range rows {
		ids[i] = rows[i].id
	}
	if err != nil {
		msg := sanitizeErr(err)
		_, _ = s.db.ExecContext(ctx, `UPDATE region_sync_outbox SET attempts = attempts + 1, last_error = $2 WHERE id = ANY($1)`, int64Array(ids), msg)
		return err
	}
	byID := make(map[string]Result, len(resp.Results))
	for _, r := range resp.Results {
		byID[r.ID] = r
	}
	grouped := map[string][]int64{}
	var missing []int64
	for _, r := range rows {
		res, ok := byID[r.ev.ID]
		if !ok {
			missing = append(missing, r.id)
			continue
		}
		key := res.Status
		if res.Status == StatusRejected {
			s.rejectedTotal.Add(1)
			slog.Error("region_sync_event_rejected_by_peer", "event_id", r.ev.ID, "type", r.ev.EntityType, "key", r.ev.EntityKey, "reason", res.Error)
			key = res.Status + "\x00" + res.Error
		}
		grouped[key] = append(grouped[key], r.id)
	}
	for key, group := range grouped {
		status, reason, _ := strings.Cut(key, "\x00")
		if _, err := s.db.ExecContext(ctx, `
			UPDATE region_sync_outbox SET acked_at = NOW(), ack_status = $2, last_error = NULLIF($3, ''), attempts = attempts + 1
			WHERE id = ANY($1) AND acked_at IS NULL`, int64Array(group), status, reason); err != nil {
			return err
		}
	}
	if len(missing) > 0 {
		_, _ = s.db.ExecContext(ctx, `UPDATE region_sync_outbox SET attempts = attempts + 1, last_error = 'missing result' WHERE id = ANY($1)`, int64Array(missing))
		return errors.New("peer response missing results")
	}
	return nil
}

func sanitizeErr(err error) string {
	msg := err.Error()
	// url.Error 会带上完整地址，地址本身不敏感，但截断防止异常大的错误串。
	if len(msg) > 300 {
		msg = msg[:300]
	}
	return msg
}

func (s *Service) recordFailure(err error) int64 {
	n := s.failures.Add(1)
	s.lastErrorNS.Store(s.now().UnixNano())
	msg := sanitizeErr(err)
	s.lastError.Store(&msg)
	if n == 1 || n%20 == 0 {
		slog.Warn("region_sync_send_failed", "peer", s.cfg.Peer.RegionID, "consecutive_failures", n, "error", msg)
	}
	return n
}

func (s *Service) recordSuccess() {
	if s.failures.Swap(0) > 0 {
		slog.Info("region_sync_peer_recovered", "peer", s.cfg.Peer.RegionID)
	}
	s.lastSuccessNS.Store(s.now().UnixNano())
}

// drainStream 发送一个流直到为空、失败或达到本轮上限。返回是否仍有积压。
func (s *Service) drainStream(ctx context.Context, stream int, maxBatches int, pace time.Duration) (bool, error) {
	for i := 0; i < maxBatches; i++ {
		rows, more, err := s.fetchBatch(ctx, stream)
		if err != nil {
			return false, err
		}
		if len(rows) == 0 {
			return false, nil
		}
		if err := s.sendBatch(ctx, rows); err != nil {
			return true, err
		}
		s.recordSuccess()
		if !more {
			return false, nil
		}
		if pace > 0 && !s.sleep(pace) {
			return true, nil
		}
	}
	return true, nil
}

// senderLoop 优先级：配置/Key 状态 > 充值退款调账 > 消费流水。
// 恢复后先把前两类补完，再补消费；每补一批消费都回头检查前两类。
func (s *Service) senderLoop() {
	defer s.wg.Done()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { <-s.stopCh; cancel() }()

	scan := time.Duration(minInt(s.cfg.ConfigScanIntervalMS, s.cfg.LedgerScanIntervalMS)) * time.Millisecond
	usageEvery := time.Duration(s.cfg.UsageFlushIntervalMS) * time.Millisecond
	pace := time.Duration(0)
	if s.cfg.CatchupMaxBatchesPerS > 0 {
		pace = time.Second / time.Duration(s.cfg.CatchupMaxBatchesPerS)
	}
	lastUsage := time.Time{}
	for {
		select {
		case <-s.stopCh:
			return
		default:
		}
		if !s.cfg.SenderEnabled {
			if !s.sleep(5 * time.Second) {
				return
			}
			continue
		}
		var sendErr error
		usageBacklog := false
		for _, stream := range []int{StreamConfig, StreamLedger} {
			if _, err := s.drainStream(ctx, stream, 1000, pace); err != nil {
				sendErr = err
				break
			}
		}
		if sendErr == nil && (time.Since(lastUsage) >= usageEvery) {
			more, err := s.drainStream(ctx, StreamUsage, 1, 0)
			if err != nil {
				sendErr = err
			} else {
				lastUsage = time.Now()
				usageBacklog = more
			}
		}
		if sendErr != nil {
			if ctx.Err() != nil {
				return
			}
			n := s.recordFailure(sendErr)
			if errors.Is(sendErr, errSchemaMismatch) {
				n = 100
			}
			if !s.sleep(s.backoff(n)) {
				return
			}
			continue
		}
		if usageBacklog {
			// 有积压：连续补发，但按节流间隔，避免抢占本地库。
			lastUsage = time.Time{}
			if pace > 0 && !s.sleep(pace) {
				return
			}
			continue
		}
		t := time.NewTimer(scan)
		select {
		case <-s.stopCh:
			t.Stop()
			return
		case <-s.wake:
		case <-t.C:
		}
		t.Stop()
	}
}

func minInt(a, b int) int {
	if a <= 0 {
		return b
	}
	if b <= 0 || a < b {
		return a
	}
	return b
}

// maintenanceLoop 缓存失效、暂存重试、对账、清理与积压告警。
func (s *Service) maintenanceLoop() {
	defer s.wg.Done()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { <-s.stopCh; cancel() }()

	invTicker := time.NewTicker(2 * time.Second)
	defer invTicker.Stop()
	reconcileEvery := time.Duration(s.cfg.ReconcileIntervalS) * time.Second
	if reconcileEvery <= 0 {
		reconcileEvery = 5 * time.Minute
	}
	reconcileTicker := time.NewTicker(reconcileEvery)
	defer reconcileTicker.Stop()
	alertTicker := time.NewTicker(time.Minute)
	defer alertTicker.Stop()

	s.drainInvalidations(ctx)
	for {
		select {
		case <-s.stopCh:
			return
		case <-s.invWake:
			s.drainInvalidations(ctx)
		case <-invTicker.C:
			s.drainInvalidations(ctx)
		case <-reconcileTicker.C:
			s.runMaintenance(ctx)
		case <-alertTicker.C:
			s.checkBacklog(ctx)
		}
	}
}

func (s *Service) drainInvalidations(ctx context.Context) {
	for i := 0; i < 20; i++ {
		n, err := DrainInvalidations(ctx, s.db, s.inv, 500)
		if err != nil {
			if ctx.Err() == nil {
				slog.Warn("region_sync_invalidation_drain_failed", "error", err)
			}
			return
		}
		if n < 500 {
			return
		}
	}
}

func (s *Service) runMaintenance(ctx context.Context) {
	if n, err := s.applier.RetryDeferred(ctx, 500); err != nil {
		slog.Warn("region_sync_retry_deferred_failed", "error", err)
	} else if n > 0 {
		slog.Info("region_sync_deferred_applied", "count", n)
		s.drainInvalidations(ctx)
	}
	if s.cfg.SenderEnabled {
		if requeued, err := s.Reconcile(ctx); err != nil {
			slog.Warn("region_sync_reconcile_failed", "error", sanitizeErr(err))
		} else if requeued > 0 {
			slog.Warn("region_sync_reconcile_requeued", "count", requeued)
			s.poke(s.wake)
		}
	}
	s.prune(ctx)
}

func (s *Service) prune(ctx context.Context) {
	if h := s.cfg.OutboxRetentionHours; h > 0 {
		_, _ = s.db.ExecContext(ctx, `
			DELETE FROM region_sync_outbox WHERE id IN (
				SELECT id FROM region_sync_outbox
				WHERE acked_at IS NOT NULL AND acked_at < NOW() - make_interval(hours => $1)
				ORDER BY id LIMIT 20000)`, h)
	}
	if h := s.cfg.InboxRetentionHours; h > 0 {
		_, _ = s.db.ExecContext(ctx, `
			DELETE FROM region_sync_inbox WHERE event_id IN (
				SELECT event_id FROM region_sync_inbox
				WHERE received_at < NOW() - make_interval(hours => $1) AND status <> 'deferred'
				LIMIT 20000)`, h)
	}
}

func (s *Service) checkBacklog(ctx context.Context) {
	st, err := s.Status(ctx)
	if err != nil {
		return
	}
	limit := float64(s.cfg.BacklogAlertAgeSeconds)
	for _, ss := range st.Streams {
		if limit > 0 && ss.OldestAgeSeconds > limit {
			slog.Warn("region_sync_backlog_alert", "stream", ss.Name, "pending", ss.Pending, "oldest_age_seconds", int64(ss.OldestAgeSeconds),
				"consecutive_failures", st.ConsecutiveFailures, "outbox_bytes", st.OutboxBytes)
		}
	}
	if st.Rejected > 0 || st.DeferredStuck > 0 {
		slog.Warn("region_sync_attention", "rejected", st.Rejected, "deferred_stuck", st.DeferredStuck)
	}
}
