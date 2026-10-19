package regionsync

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// 待执行的本地缓存失效类型。登记与落库同事务，提交后由 worker 执行；
// 进程在两者之间崩溃时，重启后从表里继续，不会漏失效。
const (
	InvUserAuth         = "user_auth"
	InvUserBalance      = "user_balance"
	InvAPIKey           = "api_key"
	InvGroup            = "group"
	InvSubscription     = "subscription"
	InvChannel          = "channel"
	InvErrorPassthrough = "error_passthrough"
	InvTLSProfile       = "tls_profile"
)

// CacheInvalidator 由服务层适配器实现（见 adapter.go）。
type CacheInvalidator interface {
	InvalidateUserAuth(ctx context.Context, userID int64)
	InvalidateUserBalance(ctx context.Context, userID int64)
	InvalidateAPIKey(ctx context.Context, keyID int64)
	InvalidateGroup(ctx context.Context, groupID int64)
	InvalidateSubscription(ctx context.Context, userID, groupID int64)
	InvalidateChannels(ctx context.Context)
	InvalidateErrorPassthrough(ctx context.Context)
	InvalidateTLSProfiles(ctx context.Context)
}

func addInvalidation(ctx context.Context, tx *sql.Tx, kind, ref string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO region_sync_pending_invalidations (kind, ref) VALUES ($1, $2)`, kind, ref)
	return err
}

func enqueueScheduler(ctx context.Context, tx *sql.Tx, eventType string, accountID, groupID any) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO scheduler_outbox (event_type, account_id, group_id) VALUES ($1, $2, $3)`, eventType, accountID, groupID)
	return err
}

func idOf(row map[string]any, col string) (int64, bool) {
	if row == nil {
		return 0, false
	}
	s, err := pkValue(row[col])
	if err != nil {
		return 0, false
	}
	id, err := strconv.ParseInt(s, 10, 64)
	return id, err == nil && id > 0
}

// enqueueInvalidations 配置行变化后要刷新的缓存与调度快照。before/after 任一可为空。
func (a *Applier) enqueueInvalidations(ctx context.Context, tx *sql.Tx, table string, before, after map[string]any) error {
	rows := []map[string]any{}
	for _, r := range []map[string]any{before, after} {
		if r != nil {
			rows = append(rows, r)
		}
	}
	seen := map[string]bool{}
	add := func(kind, ref string) error {
		k := kind + "\x00" + ref
		if seen[k] {
			return nil
		}
		seen[k] = true
		return addInvalidation(ctx, tx, kind, ref)
	}
	for _, r := range rows {
		var err error
		switch table {
		case "users":
			if id, ok := idOf(r, "id"); ok {
				err = add(InvUserAuth, strconv.FormatInt(id, 10))
			}
		case "api_keys":
			if id, ok := idOf(r, "id"); ok {
				err = add(InvAPIKey, strconv.FormatInt(id, 10))
			}
		case "groups":
			if id, ok := idOf(r, "id"); ok && !seen["sched_group"+fmt.Sprint(id)] {
				seen["sched_group"+fmt.Sprint(id)] = true
				if err = add(InvGroup, strconv.FormatInt(id, 10)); err == nil {
					err = enqueueScheduler(ctx, tx, "group_changed", nil, id)
				}
			}
		case "composite_model_routes":
			if id, ok := idOf(r, "group_id"); ok {
				err = add(InvGroup, strconv.FormatInt(id, 10))
			}
		case "user_allowed_groups", "user_group_rate_multipliers", "user_platform_quotas":
			if id, ok := idOf(r, "user_id"); ok {
				err = add(InvUserAuth, strconv.FormatInt(id, 10))
			}
		case "user_subscriptions":
			u, ok1 := idOf(r, "user_id")
			g, ok2 := idOf(r, "group_id")
			if ok1 && ok2 {
				err = add(InvSubscription, fmt.Sprintf("%d:%d", u, g))
			}
		case "accounts":
			if id, ok := idOf(r, "id"); ok && !seen["sched_acc"+fmt.Sprint(id)] {
				seen["sched_acc"+fmt.Sprint(id)] = true
				err = enqueueScheduler(ctx, tx, "account_changed", id, nil)
			}
		case "account_groups":
			if id, ok := idOf(r, "account_id"); ok && !seen["sched_accg"+fmt.Sprint(id)] {
				seen["sched_accg"+fmt.Sprint(id)] = true
				err = enqueueScheduler(ctx, tx, "account_groups_changed", id, nil)
			}
		case "proxies":
			if id, ok := idOf(r, "id"); ok && !seen["sched_proxy"+fmt.Sprint(id)] {
				seen["sched_proxy"+fmt.Sprint(id)] = true
				_, err = tx.ExecContext(ctx, `
					INSERT INTO scheduler_outbox (event_type, account_id)
					SELECT 'account_changed', id FROM accounts WHERE proxy_id = $1 AND deleted_at IS NULL`, id)
			}
		case "channels", "channel_groups", "channel_model_pricing", "channel_pricing_intervals":
			err = add(InvChannel, "0")
		case "error_passthrough_rules":
			err = add(InvErrorPassthrough, "0")
		case "tls_fingerprint_profiles":
			err = add(InvTLSProfile, "0")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *Applier) enqueueDeltaInvalidations(ctx context.Context, tx *sql.Tx, table string, after map[string]any) error {
	switch table {
	case "users":
		if id, ok := idOf(after, "id"); ok {
			return addInvalidation(ctx, tx, InvUserBalance, strconv.FormatInt(id, 10))
		}
	case "api_keys":
		if id, ok := idOf(after, "id"); ok {
			return addInvalidation(ctx, tx, InvAPIKey, strconv.FormatInt(id, 10))
		}
	case "user_subscriptions":
		u, ok1 := idOf(after, "user_id")
		g, ok2 := idOf(after, "group_id")
		if ok1 && ok2 {
			return addInvalidation(ctx, tx, InvSubscription, fmt.Sprintf("%d:%d", u, g))
		}
	}
	return nil
}

// DrainInvalidations 执行已提交的缓存失效，执行完再删记录（崩溃最多重复失效，不会漏）。
func DrainInvalidations(ctx context.Context, db *sql.DB, inv CacheInvalidator, limit int) (int, error) {
	if inv == nil {
		inv = noopInvalidator{}
	}
	rows, err := db.QueryContext(ctx, `SELECT id, kind, ref FROM region_sync_pending_invalidations ORDER BY id LIMIT $1`, limit)
	if err != nil {
		return 0, err
	}
	type item struct {
		id   int64
		kind string
		ref  string
	}
	var items []item
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.id, &it.kind, &it.ref); err != nil {
			_ = rows.Close()
			return 0, err
		}
		items = append(items, it)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(items) == 0 {
		return 0, nil
	}
	done := map[string]bool{}
	ids := make([]int64, 0, len(items))
	for _, it := range items {
		ids = append(ids, it.id)
		k := it.kind + "\x00" + it.ref
		if done[k] {
			continue
		}
		done[k] = true
		n, _ := strconv.ParseInt(it.ref, 10, 64)
		switch it.kind {
		case InvUserAuth:
			inv.InvalidateUserAuth(ctx, n)
		case InvUserBalance:
			inv.InvalidateUserBalance(ctx, n)
		case InvAPIKey:
			inv.InvalidateAPIKey(ctx, n)
		case InvGroup:
			inv.InvalidateGroup(ctx, n)
		case InvSubscription:
			parts := strings.SplitN(it.ref, ":", 2)
			if len(parts) == 2 {
				u, _ := strconv.ParseInt(parts[0], 10, 64)
				g, _ := strconv.ParseInt(parts[1], 10, 64)
				inv.InvalidateSubscription(ctx, u, g)
			}
		case InvChannel:
			inv.InvalidateChannels(ctx)
		case InvErrorPassthrough:
			inv.InvalidateErrorPassthrough(ctx)
		case InvTLSProfile:
			inv.InvalidateTLSProfiles(ctx)
		}
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM region_sync_pending_invalidations WHERE id = ANY($1)`, int64Array(ids)); err != nil {
		return 0, err
	}
	return len(items), nil
}

type noopInvalidator struct{}

func (noopInvalidator) InvalidateUserAuth(context.Context, int64)            {}
func (noopInvalidator) InvalidateUserBalance(context.Context, int64)         {}
func (noopInvalidator) InvalidateAPIKey(context.Context, int64)              {}
func (noopInvalidator) InvalidateGroup(context.Context, int64)               {}
func (noopInvalidator) InvalidateSubscription(context.Context, int64, int64) {}
func (noopInvalidator) InvalidateChannels(context.Context)                   {}
func (noopInvalidator) InvalidateErrorPassthrough(context.Context)           {}
func (noopInvalidator) InvalidateTLSProfiles(context.Context)                {}
