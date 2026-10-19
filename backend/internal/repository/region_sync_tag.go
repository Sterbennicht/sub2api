package repository

import (
	"context"
	"database/sql"

	"github.com/Wei-Shaw/sub2api/internal/pkg/region"
)

// 地区同步的余额变动分类。迁移 246 的增量触发器读取这两个事务级设置：
// reason=usage 的消费增量批量同步，其余（充值、退款、调账、冻结释放）高优先级同步。
// 未打标的余额变动一律按高优先级处理，因此新增的余额入口不会漏记。
const (
	regionSyncReasonUsage = "usage"
	regionSyncReasonHold  = "hold"
)

// tagRegionSyncReason 只在多地区开启时多发一条本地语句；单节点不做任何事。
func tagRegionSyncReason(ctx context.Context, tx *sql.Tx, reason, ref string) error {
	if !region.Enabled() {
		return nil
	}
	_, err := tx.ExecContext(ctx,
		`SELECT set_config('sub2api.region_sync_reason', $1, true), set_config('sub2api.region_sync_ref', $2, true)`,
		reason, ref)
	return err
}
