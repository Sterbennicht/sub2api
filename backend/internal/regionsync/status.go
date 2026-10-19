package regionsync

import (
	"context"
	"database/sql"
	"log/slog"
	"sort"
	"strings"
	"time"
)

// StreamStatus 单个流的积压。
type StreamStatus struct {
	Stream           int     `json:"stream"`
	Name             string  `json:"name"`
	Pending          int64   `json:"pending"`
	OldestAgeSeconds float64 `json:"oldest_age_seconds"`
}

// Status 同步健康状况（不含任何事件内容）。
type Status struct {
	Region               string         `json:"region"`
	Peer                 string         `json:"peer"`
	Primary              bool           `json:"primary"`
	SenderEnabled        bool           `json:"sender_enabled"`
	Streams              []StreamStatus `json:"streams"`
	ConsecutiveFailures  int64          `json:"consecutive_failures"`
	LastSuccessAt        *time.Time     `json:"last_success_at,omitempty"`
	LastErrorAt          *time.Time     `json:"last_error_at,omitempty"`
	LastError            string         `json:"last_error,omitempty"`
	OutboxBytes          int64          `json:"outbox_bytes"`
	InboxBytes           int64          `json:"inbox_bytes"`
	Deferred             int64          `json:"deferred"`
	DeferredStuck        int64          `json:"deferred_stuck"`
	Rejected             int64          `json:"rejected"`
	RejectedByPeer       int64          `json:"rejected_by_peer_since_start"`
	PendingInvalidations int64          `json:"pending_invalidations"`
}

var streamNames = map[int]string{StreamConfig: "config", StreamLedger: "ledger", StreamUsage: "usage"}

// QueryStatus 只读本地表，CLI 与 HTTP 共用。
func QueryStatus(ctx context.Context, db *sql.DB) (*Status, error) {
	st := &Status{}
	rows, err := db.QueryContext(ctx, `
		SELECT stream, count(*), COALESCE(EXTRACT(EPOCH FROM NOW() - min(created_at)), 0)
		FROM region_sync_outbox WHERE acked_at IS NULL GROUP BY stream`)
	if err != nil {
		return nil, err
	}
	found := map[int]StreamStatus{}
	for rows.Next() {
		var ss StreamStatus
		if err := rows.Scan(&ss.Stream, &ss.Pending, &ss.OldestAgeSeconds); err != nil {
			_ = rows.Close()
			return nil, err
		}
		found[ss.Stream] = ss
	}
	_ = rows.Close()
	for _, n := range []int{StreamConfig, StreamLedger, StreamUsage} {
		ss := found[n]
		ss.Stream, ss.Name = n, streamNames[n]
		st.Streams = append(st.Streams, ss)
	}
	_ = db.QueryRowContext(ctx, `SELECT pg_total_relation_size('region_sync_outbox'), pg_total_relation_size('region_sync_inbox')`).Scan(&st.OutboxBytes, &st.InboxBytes)
	_ = db.QueryRowContext(ctx, `
		SELECT count(*) FILTER (WHERE kind <> 'rejected'),
		       count(*) FILTER (WHERE kind <> 'rejected' AND created_at < NOW() - INTERVAL '1 hour'),
		       count(*) FILTER (WHERE kind = 'rejected')
		FROM region_sync_deferred`).Scan(&st.Deferred, &st.DeferredStuck, &st.Rejected)
	_ = db.QueryRowContext(ctx, `SELECT count(*) FROM region_sync_pending_invalidations`).Scan(&st.PendingInvalidations)
	_ = db.QueryRowContext(ctx, `SELECT region_id, is_primary FROM region_sync_local WHERE id = 1`).Scan(&st.Region, &st.Primary)
	return st, nil
}

// Status 本地状态 + 发送端运行时计数。
func (s *Service) Status(ctx context.Context) (*Status, error) {
	st, err := QueryStatus(ctx, s.db)
	if err != nil {
		return nil, err
	}
	st.Region, st.Peer, st.Primary, st.SenderEnabled = s.cfg.RegionID, s.cfg.Peer.RegionID, s.cfg.IsPrimary(), s.cfg.SenderEnabled
	st.ConsecutiveFailures = s.failures.Load()
	st.RejectedByPeer = s.rejectedTotal.Load()
	if ns := s.lastSuccessNS.Load(); ns > 0 {
		t := time.Unix(0, ns)
		st.LastSuccessAt = &t
	}
	if ns := s.lastErrorNS.Load(); ns > 0 {
		t := time.Unix(0, ns)
		st.LastErrorAt = &t
		if p := s.lastError.Load(); p != nil {
			st.LastError = *p
		}
	}
	return st, nil
}

// ---------------------------------------------------------------------------
// 对账：按已确认事件的 id 区间比对数量和摘要，发现对端缺失就重新入队。
// 不依赖「最大已发送 id」游标，晚提交、乱序、确认丢失都不会被跳过。

const reconcileChunk = 1000

// ReconcileRange 一个 id 区间的摘要。
type ReconcileRange struct {
	From   int64  `json:"from"`
	To     int64  `json:"to"`
	Count  int64  `json:"count"`
	Digest string `json:"digest"`
}

// ReconcileRequest 发送端 -> 接收端。
type ReconcileRequest struct {
	Origin string           `json:"origin"`
	Ranges []ReconcileRange `json:"ranges"`
}

// ReconcileMismatch 接收端在不一致区间里实际收到的序号。
type ReconcileMismatch struct {
	From    int64   `json:"from"`
	To      int64   `json:"to"`
	Present []int64 `json:"present"`
}

// ReconcileResponse 接收端返回。
type ReconcileResponse struct {
	Mismatches []ReconcileMismatch `json:"mismatches"`
}

func localAckedRanges(ctx context.Context, db *sql.DB, windowHours int) ([]ReconcileRange, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT (id / $1) AS chunk, count(*), md5(string_agg(id::TEXT, ',' ORDER BY id))
		FROM region_sync_outbox
		WHERE acked_at IS NOT NULL AND ack_status IS DISTINCT FROM 'baseline'
		  AND created_at > NOW() - make_interval(hours => $2)
		  AND acked_at < NOW() - INTERVAL '30 seconds'
		GROUP BY 1 ORDER BY 1`, reconcileChunk, windowHours)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []ReconcileRange
	for rows.Next() {
		var chunk int64
		var r ReconcileRange
		if err := rows.Scan(&chunk, &r.Count, &r.Digest); err != nil {
			return nil, err
		}
		r.From, r.To = chunk*reconcileChunk, chunk*reconcileChunk+reconcileChunk-1
		out = append(out, r)
	}
	return out, rows.Err()
}

// Reconcile 向对端发起对账，返回重新入队的事件数。
func (s *Service) Reconcile(ctx context.Context) (int, error) {
	s.reconcileM.Lock()
	defer s.reconcileM.Unlock()
	ranges, err := localAckedRanges(ctx, s.db, s.cfg.ReconcileWindowHours)
	if err != nil {
		return 0, err
	}
	requeued := 0
	for start := 0; start < len(ranges); start += 200 {
		end := start + 200
		if end > len(ranges) {
			end = len(ranges)
		}
		var resp ReconcileResponse
		if err := s.post(ctx, reconcilePath, ReconcileRequest{Origin: s.cfg.RegionID, Ranges: ranges[start:end]}, &resp); err != nil {
			return requeued, err
		}
		for _, m := range resp.Mismatches {
			present := make(map[int64]bool, len(m.Present))
			for _, id := range m.Present {
				present[id] = true
			}
			rows, err := s.db.QueryContext(ctx, `
				SELECT id FROM region_sync_outbox
				WHERE id BETWEEN $1 AND $2 AND acked_at IS NOT NULL AND ack_status IS DISTINCT FROM 'baseline'`, m.From, m.To)
			if err != nil {
				return requeued, err
			}
			var missing []int64
			for rows.Next() {
				var id int64
				if err := rows.Scan(&id); err != nil {
					_ = rows.Close()
					return requeued, err
				}
				if !present[id] {
					missing = append(missing, id)
				}
			}
			_ = rows.Close()
			if len(missing) == 0 {
				continue
			}
			res, err := s.db.ExecContext(ctx, `
				UPDATE region_sync_outbox SET acked_at = NULL, ack_status = NULL, last_error = 'requeued by reconcile'
				WHERE id = ANY($1)`, int64Array(missing))
			if err != nil {
				return requeued, err
			}
			n, _ := res.RowsAffected()
			requeued += int(n)
		}
	}
	return requeued, nil
}

// handleReconcile 接收端：计算本地 inbox 在各区间的摘要，只返回不一致区间的已收序号。
func handleReconcile(ctx context.Context, db *sql.DB, req *ReconcileRequest) (*ReconcileResponse, error) {
	resp := &ReconcileResponse{}
	for _, r := range req.Ranges {
		if r.To < r.From || r.To-r.From > 100000 {
			continue
		}
		var count int64
		var digest sql.NullString
		if err := db.QueryRowContext(ctx, `
			SELECT count(*), md5(string_agg(origin_seq::TEXT, ',' ORDER BY origin_seq))
			FROM region_sync_inbox WHERE origin_region = $1 AND origin_seq BETWEEN $2 AND $3`,
			req.Origin, r.From, r.To).Scan(&count, &digest); err != nil {
			return nil, err
		}
		if count == r.Count && digest.String == r.Digest {
			continue
		}
		rows, err := db.QueryContext(ctx, `
			SELECT origin_seq FROM region_sync_inbox WHERE origin_region = $1 AND origin_seq BETWEEN $2 AND $3`,
			req.Origin, r.From, r.To)
		if err != nil {
			return nil, err
		}
		m := ReconcileMismatch{From: r.From, To: r.To}
		for rows.Next() {
			var seq int64
			if err := rows.Scan(&seq); err != nil {
				_ = rows.Close()
				return nil, err
			}
			m.Present = append(m.Present, seq)
		}
		_ = rows.Close()
		resp.Mismatches = append(resp.Mismatches, m)
	}
	return resp, nil
}

// BalanceChecksum 两地积压都为 0 时用于比对余额是否收敛（只输出摘要）。
func BalanceChecksum(ctx context.Context, db *sql.DB) (string, int64, error) {
	var digest sql.NullString
	var n int64
	err := db.QueryRowContext(ctx, `
		SELECT md5(string_agg(id::TEXT || ':' || balance::TEXT || ':' || frozen_balance::TEXT, ',' ORDER BY id)), count(*)
		FROM users WHERE deleted_at IS NULL`).Scan(&digest, &n)
	return digest.String, n, err
}

// warnUnclassifiedColumns 升级后新增、但没有归类（同步字段 / 增量 / 本地状态）的列，提示需要更新注册表。
func (s *Service) warnUnclassifiedColumns() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, t := range ConfigTables {
		cols, err := s.applier.columns(ctx, s.db, t.Name)
		if err != nil {
			continue
		}
		known := map[string]bool{"id": true, "updated_at": true}
		for _, c := range t.PK {
			known[c] = true
		}
		for _, g := range t.Groups {
			for _, spec := range g.Columns {
				known[columnName(spec)] = true
			}
		}
		if dt, ok := deltaTable(t.Name); ok {
			for _, c := range dt.Columns {
				known[c.Name] = true
			}
		}
		for _, c := range LocalColumns[t.Name] {
			known[c] = true
		}
		var unknown []string
		for c := range cols {
			if !known[c] {
				unknown = append(unknown, c)
			}
		}
		if len(unknown) > 0 {
			sort.Strings(unknown)
			slog.Warn("region_sync_unclassified_columns", "table", t.Name, "columns", strings.Join(unknown, ","),
				"hint", "这些列不会被同步；如属配置请加入 regionsync.ConfigTables 并新增迁移")
		}
	}
}
