package regionsync

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/lib/pq"
)

// SchemaVersion 本版本能理解的事件格式。接收到更高版本时整批拒收（409），
// 发送端保留积压，等接收端升级后再补发。
const SchemaVersion = 1

// 事件处理结果。除 transient 错误外，这些状态都会让发送端确认（不再重发）。
const (
	StatusApplied   = "applied"
	StatusDuplicate = "duplicate" // event_id 已处理过（重复投递 / 确认丢失后重发）
	StatusStale     = "stale"     // 版本不比本地新、墓碑或不可复活的删除
	StatusDeferred  = "deferred"  // 已持久化到 region_sync_deferred，依赖就绪后自动应用
	StatusRejected  = "rejected"  // 数据错误，已存入 region_sync_deferred(kind=rejected) 待人工处理
)

const (
	StreamConfig = 1
	StreamLedger = 2
	StreamUsage  = 3
)

// Event 传输格式。Payload 原样来自源地区 outbox，接收端不重算价格。
type Event struct {
	ID            string          `json:"id"`
	Seq           int64           `json:"seq"`
	Origin        string          `json:"origin"`
	SchemaVersion int             `json:"schema_version"`
	Stream        int             `json:"stream"`
	EntityType    string          `json:"type"`
	EntityKey     string          `json:"key"`
	Payload       json.RawMessage `json:"payload"`
}

// Batch 一次推送。
type Batch struct {
	Origin        string  `json:"origin"`
	SchemaVersion int     `json:"schema_version"`
	Events        []Event `json:"events"`
}

// Result 单个事件的处理结果。Error 只含错误分类，不含负载内容。
type Result struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type groupState struct {
	V int64          `json:"v"`
	R string         `json:"r"`
	C map[string]any `json:"c"`
}

type upsertPayload struct {
	Op string                `json:"op"`
	PK map[string]any        `json:"pk"`
	G  map[string]groupState `json:"g"`
	V  int64                 `json:"v"`
	R  string                `json:"r"`
}

type deltaPayload struct {
	Op     string            `json:"op"`
	PK     map[string]any    `json:"pk"`
	D      map[string]string `json:"d"`
	Z      []string          `json:"z"`
	Reason string            `json:"reason"`
}

type usagePayload struct {
	Op  string         `json:"op"`
	Row map[string]any `json:"row"`
}

type version struct {
	ts     int64
	region string
}

func (v version) newer(o version) bool {
	if v.ts != o.ts {
		return v.ts > o.ts
	}
	return v.region > o.region
}

// Applier 在本地库里应用对端事件。所有写入都在调用方的事务里完成。
type Applier struct {
	db               *sql.DB
	localRegion      string
	primaryRegion    string
	localSettingKeys map[string]bool
	// usageAggregator 本地区是否导入对端使用记录（全局用量/账务汇总地区）。
	usageAggregator bool

	colsMu    sync.Mutex
	tableCols map[string]map[string]bool
	usageCols []string
}

// NewApplier 创建 Applier。
func NewApplier(db *sql.DB, localRegion, primaryRegion string, localSettingKeys []string) *Applier {
	keys := make(map[string]bool, len(localSettingKeys))
	for _, k := range localSettingKeys {
		if k = strings.TrimSpace(k); k != "" {
			keys[k] = true
		}
	}
	return &Applier{db: db, localRegion: localRegion, primaryRegion: primaryRegion, localSettingKeys: keys}
}

// permanentError 数据层面的错误：同一事件重试也不会成功。
type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

func permanent(format string, args ...any) error {
	return &permanentError{err: fmt.Errorf(format, args...)}
}

// pqClass 返回 PostgreSQL 错误类（前两位）。
func pqCode(err error) string {
	var pe *pq.Error
	if errors.As(err, &pe) {
		return string(pe.Code)
	}
	return ""
}

func isPermanent(err error) bool {
	var pe *permanentError
	if errors.As(err, &pe) {
		return true
	}
	code := pqCode(err)
	// 22 数据异常、23 完整性约束、42 语法/未定义对象（schema 不一致）
	return strings.HasPrefix(code, "22") || strings.HasPrefix(code, "23") || strings.HasPrefix(code, "42")
}

// errorClass 只输出错误类别，避免把负载里的凭据带进日志或对端响应。
func errorClass(err error) string {
	if code := pqCode(err); code != "" {
		var pe *pq.Error
		errors.As(err, &pe)
		detail := pe.Constraint
		if detail == "" {
			detail = pe.Table
		}
		return "pg:" + code + ":" + detail
	}
	msg := err.Error()
	if len(msg) > 200 {
		msg = msg[:200]
	}
	return msg
}

// ApplyBatch 在一个事务里完成「去重 + 应用 + 缓存失效登记」，提交后才返回结果。
// 返回 error 表示可重试的整批失败（发送端整批重发，去重保证不会重复应用）。
func (a *Applier) ApplyBatch(ctx context.Context, batch *Batch) ([]Result, error) {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if _, err := tx.ExecContext(ctx, `SELECT set_config('sub2api.region_sync_applying', 'on', true)`); err != nil {
		return nil, err
	}

	results := make([]Result, 0, len(batch.Events))
	for i := range batch.Events {
		ev := &batch.Events[i]
		res, err := a.applyOne(ctx, tx, batch.Origin, ev)
		if err != nil {
			return nil, err
		}
		results = append(results, res)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	committed = true
	return results, nil
}

func (a *Applier) applyOne(ctx context.Context, tx *sql.Tx, origin string, ev *Event) (Result, error) {
	res := Result{ID: ev.ID}
	if ev.Origin != origin || ev.Origin == a.localRegion {
		// 保留原始身份：只接受对端自己产生的事件，杜绝转发回环。
		res.Status, res.Error = StatusRejected, "origin mismatch"
		return res, nil
	}
	if _, err := tx.ExecContext(ctx, `SAVEPOINT rs_ev`); err != nil {
		return res, err
	}
	var inserted int
	err := tx.QueryRowContext(ctx, `
		INSERT INTO region_sync_inbox (event_id, origin_region, origin_seq, stream, status)
		VALUES ($1, $2, $3, $4, 'processing')
		ON CONFLICT (event_id) DO NOTHING
		RETURNING 1`, ev.ID, ev.Origin, ev.Seq, ev.Stream).Scan(&inserted)
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := tx.ExecContext(ctx, `RELEASE SAVEPOINT rs_ev`); err != nil {
			return res, err
		}
		res.Status = StatusDuplicate
		return res, nil
	}
	if err != nil {
		if isPermanent(err) {
			return a.reject(ctx, tx, ev, err)
		}
		return res, err
	}

	status, applyErr := a.dispatch(ctx, tx, ev)
	if applyErr != nil {
		if isPermanent(applyErr) {
			return a.reject(ctx, tx, ev, applyErr)
		}
		return res, applyErr
	}
	if _, err := tx.ExecContext(ctx, `UPDATE region_sync_inbox SET status = $2 WHERE event_id = $1`, ev.ID, status); err != nil {
		return res, err
	}
	if _, err := tx.ExecContext(ctx, `RELEASE SAVEPOINT rs_ev`); err != nil {
		return res, err
	}
	res.Status = status
	return res, nil
}

// reject 回滚该事件的部分写入，留下 inbox 记录（防止重复）和原始负载（供人工重放）。
func (a *Applier) reject(ctx context.Context, tx *sql.Tx, ev *Event, cause error) (Result, error) {
	res := Result{ID: ev.ID, Status: StatusRejected, Error: errorClass(cause)}
	if _, err := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT rs_ev`); err != nil {
		return res, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO region_sync_inbox (event_id, origin_region, origin_seq, stream, status)
		VALUES ($1, $2, $3, $4, 'rejected') ON CONFLICT (event_id) DO NOTHING`,
		ev.ID, ev.Origin, ev.Seq, ev.Stream); err != nil {
		return res, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO region_sync_deferred (event_id, kind, entity_type, entity_key, origin_region, payload, last_error)
		VALUES ($1, 'rejected', $2, $3, $4, $5, $6) ON CONFLICT (event_id) DO NOTHING`,
		ev.ID, ev.EntityType, ev.EntityKey, ev.Origin, []byte(ev.Payload), res.Error); err != nil {
		return res, err
	}
	if _, err := tx.ExecContext(ctx, `RELEASE SAVEPOINT rs_ev`); err != nil {
		return res, err
	}
	return res, nil
}

func decodeJSON(raw []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	return dec.Decode(v)
}

func (a *Applier) dispatch(ctx context.Context, tx *sql.Tx, ev *Event) (string, error) {
	var head struct {
		Op string `json:"op"`
	}
	if err := json.Unmarshal(ev.Payload, &head); err != nil {
		return "", permanent("bad payload")
	}
	switch head.Op {
	case "upsert", "delete":
		t, ok := configTable(ev.EntityType)
		if !ok {
			return "", permanent("unknown config table %q", ev.EntityType)
		}
		var p upsertPayload
		if err := decodeJSON(ev.Payload, &p); err != nil {
			return "", permanent("bad payload")
		}
		if head.Op == "delete" {
			return a.applyDelete(ctx, tx, t, ev, &p)
		}
		return a.applyUpsert(ctx, tx, t, ev, &p)
	case "delta":
		t, ok := deltaTable(ev.EntityType)
		if !ok {
			return "", permanent("unknown delta table %q", ev.EntityType)
		}
		var p deltaPayload
		if err := decodeJSON(ev.Payload, &p); err != nil {
			return "", permanent("bad payload")
		}
		status, err := a.applyDelta(ctx, tx, t, ev, &p)
		if err != nil {
			return "", err
		}
		if status == StatusDeferred {
			if err := a.deferEvent(ctx, tx, ev, "delta"); err != nil {
				return "", err
			}
		}
		return status, nil
	case "usage_log":
		if !a.usageAggregator {
			return "", permanent("usage logs are only imported by the usage aggregation region")
		}
		var p usagePayload
		if err := decodeJSON(ev.Payload, &p); err != nil {
			return "", permanent("bad payload")
		}
		return a.applyUsageLog(ctx, tx, ev, &p, true)
	}
	return "", permanent("unknown op %q", head.Op)
}

func (a *Applier) deferEvent(ctx context.Context, tx *sql.Tx, ev *Event, kind string) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO region_sync_deferred (event_id, kind, entity_type, entity_key, origin_region, payload)
		VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (event_id) DO NOTHING`,
		ev.ID, kind, ev.EntityType, ev.EntityKey, ev.Origin, []byte(ev.Payload))
	return err
}

// ---------------------------------------------------------------------------
// 列元数据

func (a *Applier) columns(ctx context.Context, q queryer, table string) (map[string]bool, error) {
	a.colsMu.Lock()
	defer a.colsMu.Unlock()
	if a.tableCols == nil {
		a.tableCols = map[string]map[string]bool{}
	}
	if c, ok := a.tableCols[table]; ok {
		return c, nil
	}
	rows, err := q.QueryContext(ctx, `
		SELECT column_name FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = $1`, table)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	set := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		set[name] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	a.tableCols[table] = set
	return set, nil
}

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// ---------------------------------------------------------------------------
// 配置

func pkValue(v any) (string, error) {
	switch t := v.(type) {
	case json.Number:
		return t.String(), nil
	case string:
		return t, nil
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), nil
	}
	return "", permanent("bad primary key value")
}

func pkWhere(pk []string, values map[string]any, startArg int) (string, []any, error) {
	parts := make([]string, 0, len(pk))
	args := make([]any, 0, len(pk))
	for i, c := range pk {
		v, ok := values[c]
		if !ok {
			return "", nil, permanent("missing primary key column %s", c)
		}
		s, err := pkValue(v)
		if err != nil {
			return "", nil, err
		}
		parts = append(parts, fmt.Sprintf("%s = $%d", quoteIdent(c), startArg+i))
		args = append(args, s)
	}
	return strings.Join(parts, " AND "), args, nil
}

func entityKeyOf(pk []string, values map[string]any) (string, error) {
	parts := make([]string, 0, len(pk))
	for _, c := range pk {
		s, err := pkValue(values[c])
		if err != nil {
			return "", err
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, "|"), nil
}

func (a *Applier) loadVersions(ctx context.Context, tx *sql.Tx, table, key string) (map[string]version, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT field_group, v_ts, v_region FROM region_sync_versions
		WHERE entity_type = $1 AND entity_key = $2`, table, key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]version{}
	for rows.Next() {
		var g string
		var v version
		if err := rows.Scan(&g, &v.ts, &v.region); err != nil {
			return nil, err
		}
		out[g] = v
	}
	return out, rows.Err()
}

func (a *Applier) storeVersion(ctx context.Context, tx *sql.Tx, table, key, group string, v version) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO region_sync_versions (entity_type, entity_key, field_group, v_ts, v_region)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (entity_type, entity_key, field_group)
		DO UPDATE SET v_ts = EXCLUDED.v_ts, v_region = EXCLUDED.v_region
		WHERE (region_sync_versions.v_ts, region_sync_versions.v_region) < (EXCLUDED.v_ts, EXCLUDED.v_region)`,
		table, key, group, v.ts, v.region)
	return err
}

func (a *Applier) lockRow(ctx context.Context, tx *sql.Tx, t *ConfigTable, pk map[string]any) (map[string]any, error) {
	where, args, err := pkWhere(t.PK, pk, 1)
	if err != nil {
		return nil, err
	}
	var raw []byte
	err = tx.QueryRowContext(ctx, fmt.Sprintf(`SELECT to_jsonb(t) FROM %s t WHERE %s FOR UPDATE`, quoteIdent(t.Name), where), args...).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	row := map[string]any{}
	if err := decodeJSON(raw, &row); err != nil {
		return nil, err
	}
	return row, nil
}

// groupAllowed 字段组归属校验：管理字段只接受主地区发出的版本。
func (a *Applier) groupAllowed(g FieldGroup, origin string) bool {
	switch g.Owner {
	case OwnerPrimary:
		return origin == a.primaryRegion
	case OwnerAny, OwnerInsert:
		return true
	}
	return false
}

func acceptValue(t *ConfigTable, col string, v any) bool {
	allowed, ok := t.AcceptValues[col]
	if !ok {
		return true
	}
	s, _ := v.(string)
	for _, a := range allowed {
		if a == s {
			return true
		}
	}
	return false
}

// mergeLocalKeys 把本地运行状态键覆盖回同步来的 jsonb。
func mergeLocalKeys(incoming any, local any, keys []string) any {
	in, _ := incoming.(map[string]any)
	if in == nil {
		in = map[string]any{}
	}
	merged := make(map[string]any, len(in)+len(keys))
	for k, v := range in {
		merged[k] = v
	}
	if lm, ok := local.(map[string]any); ok {
		for _, k := range keys {
			if v, ok := lm[k]; ok {
				merged[k] = v
			}
		}
	}
	return merged
}

func (a *Applier) applyUpsert(ctx context.Context, tx *sql.Tx, t *ConfigTable, ev *Event, p *upsertPayload) (string, error) {
	key, err := entityKeyOf(t.PK, p.PK)
	if err != nil {
		return "", err
	}
	if t.Name == "settings" && a.localSettingKeys[key] {
		return StatusStale, nil
	}
	versions, err := a.loadVersions(ctx, tx, t.Name, key)
	if err != nil {
		return "", err
	}
	row, err := a.lockRow(ctx, tx, t, p.PK)
	if err != nil {
		return "", err
	}
	tableCols, err := a.columns(ctx, tx, t.Name)
	if err != nil {
		return "", err
	}

	if row == nil {
		return a.insertRow(ctx, tx, t, ev, p, key, versions, tableCols)
	}

	if t.StickyDeleteColumn != "" && row[t.StickyDeleteColumn] != nil {
		// 本地已删除：任何事件都不能复活。
		return StatusStale, nil
	}

	set := map[string]any{}
	applied := map[string]version{}
	for _, g := range t.Groups {
		if g.Owner == OwnerInsert || !a.groupAllowed(g, ev.Origin) {
			continue
		}
		st, ok := p.G[g.Name]
		if !ok || st.V == 0 {
			continue
		}
		in := version{st.V, st.R}
		if !in.newer(versions[g.Name]) {
			continue
		}
		for _, spec := range g.Columns {
			col := columnName(spec)
			if !tableCols[col] {
				continue
			}
			v, ok := st.C[col]
			if !ok {
				continue
			}
			if !acceptValue(t, col, v) {
				continue
			}
			if keys := columnLocalKeys(spec); len(keys) > 0 {
				v = mergeLocalKeys(v, row[col], keys)
			}
			set[col] = v
		}
		applied[g.Name] = in
	}
	if len(applied) == 0 {
		return StatusStale, nil
	}
	for g, v := range applied {
		if err := a.storeVersion(ctx, tx, t.Name, key, g, v); err != nil {
			return "", err
		}
	}
	if len(set) > 0 {
		if err := a.updateRow(ctx, tx, t, p.PK, set, tableCols); err != nil {
			return "", err
		}
		after, err := a.lockRow(ctx, tx, t, p.PK)
		if err != nil {
			return "", err
		}
		if err := a.enqueueInvalidations(ctx, tx, t.Name, row, after); err != nil {
			return "", err
		}
	}
	return StatusApplied, nil
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (a *Applier) updateRow(ctx context.Context, tx *sql.Tx, t *ConfigTable, pk map[string]any, set map[string]any, tableCols map[string]bool) error {
	cols := sortedKeys(set)
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = quoteIdent(c)
	}
	doc, err := json.Marshal(set)
	if err != nil {
		return err
	}
	where, args, err := pkWhere(t.PK, pk, 2)
	if err != nil {
		return err
	}
	extra := ""
	if tableCols["updated_at"] {
		extra = ", updated_at = NOW()"
	}
	q := fmt.Sprintf(`UPDATE %s AS t SET (%s) = (SELECT %s FROM jsonb_populate_record(NULL::%s, $1::jsonb))%s WHERE %s`,
		quoteIdent(t.Name), strings.Join(quoted, ", "), strings.Join(quoted, ", "), quoteIdent(t.Name), extra, where)
	_, err = tx.ExecContext(ctx, q, append([]any{string(doc)}, args...)...)
	return err
}

func (a *Applier) insertRow(ctx context.Context, tx *sql.Tx, t *ConfigTable, ev *Event, p *upsertPayload, key string, versions map[string]version, tableCols map[string]bool) (string, error) {
	var maxIn version
	for _, st := range p.G {
		if v := (version{st.V, st.R}); v.newer(maxIn) {
			maxIn = v
		}
	}
	if tomb, ok := versions["_row"]; ok && !maxIn.newer(tomb) {
		return StatusStale, nil
	}
	doc := map[string]any{}
	for _, c := range t.PK {
		doc[c] = p.PK[c]
	}
	for _, g := range t.Groups {
		st, ok := p.G[g.Name]
		if !ok {
			continue
		}
		if g.Owner == OwnerPrimary && ev.Origin != a.primaryRegion {
			continue
		}
		for _, spec := range g.Columns {
			col := columnName(spec)
			if v, ok := st.C[col]; ok && tableCols[col] {
				doc[col] = v
			}
		}
	}
	cols := sortedKeys(doc)
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = quoteIdent(c)
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}
	q := fmt.Sprintf(`INSERT INTO %s (%s) SELECT %s FROM jsonb_populate_record(NULL::%s, $1::jsonb)`,
		quoteIdent(t.Name), strings.Join(quoted, ", "), strings.Join(quoted, ", "), quoteIdent(t.Name))
	if _, err := tx.ExecContext(ctx, q, string(raw)); err != nil {
		return "", err
	}
	for _, g := range t.Groups {
		if st, ok := p.G[g.Name]; ok && st.V > 0 && a.groupAllowed(g, ev.Origin) {
			if err := a.storeVersion(ctx, tx, t.Name, key, g.Name, version{st.V, st.R}); err != nil {
				return "", err
			}
		}
	}
	after, err := a.lockRow(ctx, tx, t, p.PK)
	if err != nil {
		return "", err
	}
	if err := a.enqueueInvalidations(ctx, tx, t.Name, nil, after); err != nil {
		return "", err
	}
	// 行刚出现：先前因为行不存在而暂存的增量现在可以落地了。
	if err := a.retryDeferredFor(ctx, tx, t.Name, key); err != nil {
		return "", err
	}
	return StatusApplied, nil
}

func (a *Applier) applyDelete(ctx context.Context, tx *sql.Tx, t *ConfigTable, ev *Event, p *upsertPayload) (string, error) {
	if t.DeleteOwner == OwnerPrimary && ev.Origin != a.primaryRegion {
		return StatusStale, nil
	}
	key, err := entityKeyOf(t.PK, p.PK)
	if err != nil {
		return "", err
	}
	if t.Name == "settings" && a.localSettingKeys[key] {
		return StatusStale, nil
	}
	versions, err := a.loadVersions(ctx, tx, t.Name, key)
	if err != nil {
		return "", err
	}
	in := version{p.V, p.R}
	for _, v := range versions {
		if !in.newer(v) {
			return StatusStale, nil
		}
	}
	row, err := a.lockRow(ctx, tx, t, p.PK)
	if err != nil {
		return "", err
	}
	if row != nil {
		where, args, err := pkWhere(t.PK, p.PK, 1)
		if err != nil {
			return "", err
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE %s`, quoteIdent(t.Name), where), args...); err != nil {
			return "", err
		}
		if err := a.enqueueInvalidations(ctx, tx, t.Name, row, nil); err != nil {
			return "", err
		}
	}
	if err := a.storeVersion(ctx, tx, t.Name, key, "_row", in); err != nil {
		return "", err
	}
	return StatusApplied, nil
}

// ---------------------------------------------------------------------------
// 增量

func (a *Applier) applyDelta(ctx context.Context, tx *sql.Tx, t *DeltaTable, ev *Event, p *deltaPayload) (string, error) {
	allowed := map[string]bool{}
	for _, c := range t.Columns {
		allowed[c.Name] = true
	}
	setParts := []string{}
	args := []any{}
	for _, col := range sortedStringKeys(p.D) {
		if !allowed[col] {
			return "", permanent("delta column %s not allowed", col)
		}
		// 校验是十进制数，按 numeric 精确相加。
		if _, err := strconv.ParseFloat(p.D[col], 64); err != nil {
			return "", permanent("bad delta amount")
		}
		args = append(args, p.D[col])
		setParts = append(setParts, fmt.Sprintf("%s = %s + $%d::numeric", quoteIdent(col), quoteIdent(col), len(args)))
	}
	for _, col := range p.Z {
		if !allowed[col] {
			return "", permanent("reset column %s not allowed", col)
		}
		setParts = append(setParts, fmt.Sprintf("%s = 0", quoteIdent(col)))
	}
	if len(setParts) == 0 {
		return StatusStale, nil
	}
	where, pkArgs, err := pkWhere(t.PK, p.PK, len(args)+1)
	if err != nil {
		return "", err
	}
	args = append(args, pkArgs...)
	q := fmt.Sprintf(`UPDATE %s AS t SET %s WHERE %s RETURNING to_jsonb(t)`, quoteIdent(t.Name), strings.Join(setParts, ", "), where)
	var raw []byte
	err = tx.QueryRowContext(ctx, q, args...).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return StatusDeferred, nil
	}
	if err != nil {
		return "", err
	}
	after := map[string]any{}
	if err := decodeJSON(raw, &after); err != nil {
		return "", err
	}
	if err := a.enqueueDeltaInvalidations(ctx, tx, t.Name, after); err != nil {
		return "", err
	}
	return StatusApplied, nil
}

func sortedStringKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ---------------------------------------------------------------------------
// 使用记录（只在汇总地区导入，用于全局用量/账务汇总；不影响余额）

func (a *Applier) usageColumns(ctx context.Context, q queryer) ([]string, error) {
	a.colsMu.Lock()
	cached := a.usageCols
	a.colsMu.Unlock()
	if cached != nil {
		return cached, nil
	}
	set, err := a.columns(ctx, q, "usage_logs")
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(set))
	for c := range set {
		if c != "id" {
			out = append(out, c)
		}
	}
	sort.Strings(out)
	a.colsMu.Lock()
	a.usageCols = out
	a.colsMu.Unlock()
	return out, nil
}

func (a *Applier) applyUsageLog(ctx context.Context, tx *sql.Tx, ev *Event, p *usagePayload, allowDefer bool) (string, error) {
	originID, err := pkValue(p.Row["id"])
	if err != nil {
		return "", err
	}
	all, err := a.usageColumns(ctx, tx)
	if err != nil {
		return "", err
	}
	cols := make([]string, 0, len(all))
	for _, c := range all {
		if _, ok := p.Row[c]; ok {
			cols = append(cols, quoteIdent(c))
		}
	}
	doc, err := json.Marshal(p.Row)
	if err != nil {
		return "", err
	}
	if _, err := tx.ExecContext(ctx, `SAVEPOINT rs_usage`); err != nil {
		return "", err
	}
	// 一条语句完成「已导入则跳过 + 插入 + 记映射」，追平积压时少几次往返。
	// 主查询看不到同语句 CTE 的写入，mapped 反映的是导入前是否已有映射。
	var localID sql.NullInt64
	var mapped bool
	colList := strings.Join(cols, ", ")
	err = tx.QueryRowContext(ctx, fmt.Sprintf(`
		WITH ins AS (
			INSERT INTO usage_logs (%s) SELECT %s FROM jsonb_populate_record(NULL::usage_logs, $1::jsonb)
			WHERE NOT EXISTS (SELECT 1 FROM region_sync_usage_import WHERE origin_region = $2 AND origin_id = $3::bigint)
			ON CONFLICT DO NOTHING RETURNING id
		), m AS (
			INSERT INTO region_sync_usage_import (origin_region, origin_id, local_id)
			SELECT $2, $3::bigint, id FROM ins ON CONFLICT DO NOTHING
		)
		SELECT (SELECT id FROM ins),
			EXISTS (SELECT 1 FROM region_sync_usage_import WHERE origin_region = $2 AND origin_id = $3::bigint)`,
		colList, colList), string(doc), ev.Origin, originID).Scan(&localID, &mapped)
	if err != nil {
		if pqCode(err) == "23503" && allowDefer {
			// 外键（用户/Key/账号）尚未同步到：暂存，配置到达后重试。
			if _, rbErr := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT rs_usage`); rbErr != nil {
				return "", rbErr
			}
			if err := a.deferEvent(ctx, tx, ev, "usage"); err != nil {
				return "", err
			}
			return StatusDeferred, nil
		}
		return "", err
	}
	if _, err := tx.ExecContext(ctx, `RELEASE SAVEPOINT rs_usage`); err != nil {
		return "", err
	}
	switch {
	case localID.Valid:
		return StatusApplied, nil
	case mapped:
		return StatusDuplicate, nil
	}
	// 同一 (request_id, api_key_id) 已存在：记映射，避免以后重复尝试。
	var existing int64
	if err := tx.QueryRowContext(ctx, `SELECT id FROM usage_logs WHERE request_id = $1 AND api_key_id = $2`,
		p.Row["request_id"], fmt.Sprint(p.Row["api_key_id"])).Scan(&existing); err == nil {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO region_sync_usage_import (origin_region, origin_id, local_id)
			VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, ev.Origin, originID, existing); err != nil {
			return "", err
		}
	}
	return StatusDuplicate, nil
}

// ---------------------------------------------------------------------------
// 暂存事件重试

func (a *Applier) retryDeferredFor(ctx context.Context, tx *sql.Tx, entityType, entityKey string) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, event_id, kind, origin_region, payload FROM region_sync_deferred
		WHERE kind = 'delta' AND entity_type = $1 AND entity_key = $2 ORDER BY id`, entityType, entityKey)
	if err != nil {
		return err
	}
	type item struct {
		id      int64
		ev      Event
		kind    string
		payload []byte
	}
	var items []item
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.id, &it.ev.ID, &it.kind, &it.ev.Origin, &it.payload); err != nil {
			_ = rows.Close()
			return err
		}
		it.ev.EntityType, it.ev.EntityKey, it.ev.Payload = entityType, entityKey, it.payload
		items = append(items, it)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, it := range items {
		t, ok := deltaTable(entityType)
		if !ok {
			continue
		}
		var p deltaPayload
		if err := decodeJSON(it.payload, &p); err != nil {
			continue
		}
		status, err := a.applyDelta(ctx, tx, t, &it.ev, &p)
		if err != nil {
			return err
		}
		if status == StatusApplied {
			if _, err := tx.ExecContext(ctx, `DELETE FROM region_sync_deferred WHERE id = $1`, it.id); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE region_sync_inbox SET status = 'applied' WHERE event_id = $1`, it.ev.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

// RetryDeferred 周期性重试暂存事件（增量与使用记录）。返回成功应用的条数。
func (a *Applier) RetryDeferred(ctx context.Context, limit int) (int, error) {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `SELECT set_config('sub2api.region_sync_applying', 'on', true)`); err != nil {
		return 0, err
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT id, event_id, kind, entity_type, entity_key, origin_region, payload FROM region_sync_deferred
		WHERE kind IN ('delta', 'usage') ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return 0, err
	}
	type item struct {
		id   int64
		kind string
		ev   Event
	}
	var items []item
	for rows.Next() {
		var it item
		var payload []byte
		if err := rows.Scan(&it.id, &it.ev.ID, &it.kind, &it.ev.EntityType, &it.ev.EntityKey, &it.ev.Origin, &payload); err != nil {
			_ = rows.Close()
			return 0, err
		}
		it.ev.Payload = payload
		items = append(items, it)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	done := 0
	for _, it := range items {
		if _, err := tx.ExecContext(ctx, `SAVEPOINT rs_retry`); err != nil {
			return done, err
		}
		status := ""
		var applyErr error
		switch it.kind {
		case "delta":
			if t, ok := deltaTable(it.ev.EntityType); ok {
				var p deltaPayload
				if err := decodeJSON(it.ev.Payload, &p); err == nil {
					status, applyErr = a.applyDelta(ctx, tx, t, &it.ev, &p)
				}
			}
		case "usage":
			var p usagePayload
			if err := decodeJSON(it.ev.Payload, &p); err == nil {
				status, applyErr = a.applyUsageLog(ctx, tx, &it.ev, &p, false)
			}
		}
		if applyErr != nil || (status != StatusApplied && status != StatusDuplicate) {
			if _, err := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT rs_retry`); err != nil {
				return done, err
			}
			msg := ""
			if applyErr != nil {
				msg = errorClass(applyErr)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE region_sync_deferred SET attempts = attempts + 1, last_error = NULLIF($2, '') WHERE id = $1`, it.id, msg); err != nil {
				return done, err
			}
			continue
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM region_sync_deferred WHERE id = $1`, it.id); err != nil {
			return done, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE region_sync_inbox SET status = $2 WHERE event_id = $1`, it.ev.ID, status); err != nil {
			return done, err
		}
		if _, err := tx.ExecContext(ctx, `RELEASE SAVEPOINT rs_retry`); err != nil {
			return done, err
		}
		done++
	}
	return done, tx.Commit()
}
