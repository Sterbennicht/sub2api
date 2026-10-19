package regionsync

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

// CLIName 服务二进制里的子命令名：sub2api region-sync <cmd>。
const CLIName = "region-sync"

const cliUsage = `用法: sub2api region-sync <命令> [参数]

  status                         本地同步状态（积压、暂存、拒绝、表大小）
  checksum                       余额摘要；两地积压都为 0 时比对是否收敛
  partition-sequences --offset N [--step 10] [--execute]
                                 把两地都会插入的表的主键序列按地区错开（默认只打印计划）
  baseline-snapshot              在源地区导出快照、标记快照内事件为 baseline，并保持快照直到回车
  baseline-import --source ID [--execute]
                                 在目标地区恢复数据后清理同步队列，准备以本地区身份启动
  replay-rejected [--limit 100] [--execute]
                                 修复原因后重放被拒绝的事件

配置从 config.yaml 读取（与服务相同的查找规则）。`

// RunCLI 执行运维子命令，返回进程退出码。
func RunCLI(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		_, _ = fmt.Fprintln(stdout, cliUsage)
		return 0
	}
	cmd, rest := args[0], args[1:]
	cfg, err := config.LoadForBootstrap()
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "load config: %v\n", err)
		return 1
	}
	db, err := sql.Open("postgres", cfg.Database.DSN())
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "open database: %v\n", err)
		return 1
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(4)
	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		_, _ = fmt.Fprintf(stderr, "connect database: %v\n", err)
		return 1
	}
	c := &cli{cfg: cfg.RegionSync, db: db, out: stdout, in: os.Stdin}
	switch cmd {
	case "status":
		err = c.status(ctx)
	case "checksum":
		err = c.checksum(ctx)
	case "partition-sequences":
		err = c.partitionSequences(ctx, rest)
	case "baseline-snapshot":
		err = c.baselineSnapshot(ctx)
	case "baseline-import":
		err = c.baselineImport(ctx, rest)
	case "replay-rejected":
		err = c.replayRejected(ctx, rest)
	default:
		_, _ = fmt.Fprintf(stderr, "未知命令 %q\n\n%s\n", cmd, cliUsage)
		return 2
	}
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "%s: %v\n", cmd, err)
		return 1
	}
	return 0
}

type cli struct {
	cfg config.RegionSyncConfig
	db  *sql.DB
	out io.Writer
	in  io.Reader
}

func (c *cli) printf(format string, args ...any) { _, _ = fmt.Fprintf(c.out, format, args...) }

func (c *cli) status(ctx context.Context) error {
	st, err := QueryStatus(ctx, c.db)
	if err != nil {
		return err
	}
	var local struct {
		Region    sql.NullString
		Primary   sql.NullBool
		Enabled   sql.NullBool
		ShipUsage sql.NullBool
	}
	err = c.db.QueryRowContext(ctx, `SELECT region_id, is_primary, enabled, ship_usage FROM region_sync_local WHERE id = 1`).
		Scan(&local.Region, &local.Primary, &local.Enabled, &local.ShipUsage)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	out := map[string]any{
		"local_row": map[string]any{"region_id": local.Region.String, "is_primary": local.Primary.Bool, "capture_enabled": local.Enabled.Bool, "ship_usage": local.ShipUsage.Bool},
		"status":    st,
	}
	enc := json.NewEncoder(c.out)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

func (c *cli) checksum(ctx context.Context) error {
	digest, n, err := BalanceChecksum(ctx, c.db)
	if err != nil {
		return err
	}
	c.printf("users=%d balance_digest=%s\n", n, digest)
	return nil
}

// partitionSequences 把 IDPartitionedTables 的序列改成「步长 step、余数 offset」，两地余数不同就不会撞主键。
// 新起点取当前最大 id 与序列位置之后、满足余数的第一个值，再留 step*1000 的余量。
func (c *cli) partitionSequences(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("partition-sequences", flag.ContinueOnError)
	offset := fs.Int64("offset", -1, "本地区余数（0..step-1），两地必须不同")
	step := fs.Int64("step", 10, "步长")
	execute := fs.Bool("execute", false, "执行（默认只打印计划）")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *step < 2 || *offset < 0 || *offset >= *step {
		return fmt.Errorf("--offset 必须在 0..%d 之间", *step-1)
	}
	for _, table := range IDPartitionedTables {
		var seq sql.NullString
		if err := c.db.QueryRowContext(ctx, `SELECT pg_get_serial_sequence($1, 'id')`, table).Scan(&seq); err != nil {
			return err
		}
		if !seq.Valid {
			return fmt.Errorf("%s.id 没有关联序列", table)
		}
		var maxID, last int64
		if err := c.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT COALESCE(MAX(id), 0) FROM %s`, quoteIdent(table))).Scan(&maxID); err != nil {
			return err
		}
		if err := c.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT last_value FROM %s`, seq.String)).Scan(&last); err != nil {
			return err
		}
		floor := maxID
		if last > floor {
			floor = last
		}
		start := floor + *step*1000
		start += ((*offset - start%*step) + *step) % *step
		c.printf("%-22s seq=%s max_id=%d last_value=%d -> INCREMENT BY %d RESTART WITH %d\n", table, seq.String, maxID, last, *step, start)
		if *execute {
			if _, err := c.db.ExecContext(ctx, fmt.Sprintf(`ALTER SEQUENCE %s INCREMENT BY %d RESTART WITH %d`, seq.String, *step, start)); err != nil {
				return err
			}
		}
	}
	if !*execute {
		c.printf("（未执行；确认后加 --execute）\n")
	}
	return nil
}

// baselineSnapshot 在源地区执行：导出一致性快照供 pg_dump 使用，把快照内已有的待发事件标为 baseline
// （它们的效果已经在快照里，目标地区不能再应用一次），快照之后提交的事件照常发送。
func (c *cli) baselineSnapshot(ctx context.Context) error {
	var enabled bool
	var regionID string
	err := c.db.QueryRowContext(ctx, `SELECT enabled, region_id FROM region_sync_local WHERE id = 1`).Scan(&enabled, &regionID)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !enabled) {
		return errors.New("本地区尚未开启捕获：先以 region_sync.enabled=true、sender_enabled=false 启动一次服务")
	}
	if err != nil {
		return err
	}
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY`); err != nil {
		return err
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), `ROLLBACK`) }()
	var snapID, snap string
	if err := conn.QueryRowContext(ctx, `SELECT pg_export_snapshot(), pg_current_snapshot()::TEXT`).Scan(&snapID, &snap); err != nil {
		return err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `INSERT INTO region_sync_baseline (source_region, snapshot, note) VALUES ($1, $2::pg_snapshot, $3)`,
		regionID, snap, "baseline-snapshot "+time.Now().UTC().Format(time.RFC3339)); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE region_sync_outbox SET acked_at = NOW(), ack_status = 'baseline'
		WHERE acked_at IS NULL AND pg_visible_in_snapshot(txid, $1::pg_snapshot)`, snap)
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	marked, _ := res.RowsAffected()
	c.printf("快照 %s（%s）已导出；%d 条快照内事件标记为 baseline。\n", snapID, snap, marked)
	c.printf("保持本命令运行，在另一个终端执行：\n\n")
	c.printf("  pg_dump -Fc --snapshot=%s \\\n", snapID)
	for _, t := range baselineExcludedData {
		c.printf("    --exclude-table-data=%s \\\n", t)
	}
	c.printf("    -d <源库> -f region-baseline.dump\n\n")
	c.printf("pg_dump 结束后按回车释放快照。\n")
	_, _ = bufio.NewReader(c.in).ReadString('\n')
	return nil
}

// 基线导出时不带数据的表：本地区身份、队列和暂存属于源地区自己；版本与墓碑、使用记录映射要带上。
var baselineExcludedData = []string{
	"region_sync_local", "region_sync_outbox", "region_sync_inbox", "region_sync_deferred",
	"region_sync_pending_invalidations", "region_sync_baseline",
}

// baselineImport 在目标地区恢复基线后执行：清理不属于本地区的同步队列，确认捕获未开启。
func (c *cli) baselineImport(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("baseline-import", flag.ContinueOnError)
	source := fs.String("source", "", "基线来源地区")
	execute := fs.Bool("execute", false, "执行（默认只检查）")
	if err := fs.Parse(args); err != nil {
		return err
	}
	src := strings.ToLower(strings.TrimSpace(*source))
	if src == "" {
		return errors.New("--source 必填")
	}
	if c.cfg.RegionID != "" && c.cfg.RegionID == src {
		return fmt.Errorf("本地区配置 region_id=%s 与 --source 相同：该命令应在目标地区执行", src)
	}
	var regionID sql.NullString
	err := c.db.QueryRowContext(ctx, `SELECT region_id FROM region_sync_local WHERE id = 1`).Scan(&regionID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if regionID.Valid && regionID.String == src {
		c.printf("警告：region_sync_local 带着源地区 %s 的身份（导出时未排除该表数据），将被清除。\n", src)
	}
	counts := map[string]int64{}
	for _, t := range baselineExcludedData {
		var n int64
		if err := c.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM %s`, t)).Scan(&n); err != nil {
			return err
		}
		counts[t] = n
		c.printf("%-36s %d 行\n", t, n)
	}
	var versions, imports int64
	_ = c.db.QueryRowContext(ctx, `SELECT count(*) FROM region_sync_versions`).Scan(&versions)
	_ = c.db.QueryRowContext(ctx, `SELECT count(*) FROM region_sync_usage_import`).Scan(&imports)
	c.printf("保留 region_sync_versions %d 行（版本与墓碑）、region_sync_usage_import %d 行\n", versions, imports)
	if !*execute {
		c.printf("（未执行；确认后加 --execute）\n")
		return nil
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `TRUNCATE `+strings.Join(baselineExcludedData, ", ")); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	c.printf("已清理。下一步：partition-sequences --offset <本地区余数> --execute，然后以本地区配置启动服务。\n")
	return nil
}

// replayRejected 重放被拒绝的事件（例如缺列、约束冲突修好之后）。每条在一个事务里完成：
// 删掉旧的拒绝记录与 inbox 记录，再走正常应用流程；仍失败会重新记为拒绝。
func (c *cli) replayRejected(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("replay-rejected", flag.ContinueOnError)
	limit := fs.Int("limit", 100, "最多重放条数")
	execute := fs.Bool("execute", false, "执行（默认只列出）")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !c.cfg.Enabled {
		return errors.New("region_sync.enabled=false")
	}
	rows, err := c.db.QueryContext(ctx, `
		SELECT d.event_id, d.origin_region, d.entity_type, d.entity_key, d.payload, d.last_error,
			COALESCE(i.origin_seq, 0), COALESCE(i.stream, 1)
		FROM region_sync_deferred d LEFT JOIN region_sync_inbox i ON i.event_id = d.event_id
		WHERE d.kind = 'rejected' ORDER BY d.id LIMIT $1`, *limit)
	if err != nil {
		return err
	}
	var events []Event
	for rows.Next() {
		var ev Event
		var lastErr sql.NullString
		if err := rows.Scan(&ev.ID, &ev.Origin, &ev.EntityType, &ev.EntityKey, &ev.Payload, &lastErr, &ev.Seq, &ev.Stream); err != nil {
			_ = rows.Close()
			return err
		}
		ev.SchemaVersion = SchemaVersion
		events = append(events, ev)
		c.printf("%s %s %s/%s %s\n", ev.ID, ev.Origin, ev.EntityType, ev.EntityKey, lastErr.String)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if !*execute {
		c.printf("%d 条（未执行；确认后加 --execute）\n", len(events))
		return nil
	}
	ap := NewApplier(c.db, c.cfg.RegionID, c.cfg.PrimaryRegion, c.cfg.LocalSettingKeys)
	ap.usageAggregator = c.cfg.IsUsageAggregator()
	results := map[string]int{}
	for i := range events {
		status, err := ap.replay(ctx, &events[i])
		if err != nil {
			return err
		}
		results[status]++
	}
	c.printf("重放结果: %v（缓存失效由运行中的服务在 2 秒内处理）\n", results)
	return nil
}

// replay 在一个事务里清掉旧记录并重新应用一条事件。
func (a *Applier) replay(ctx context.Context, ev *Event) (string, error) {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `SELECT set_config('sub2api.region_sync_applying', 'on', true)`); err != nil {
		return "", err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM region_sync_deferred WHERE event_id = $1`, ev.ID); err != nil {
		return "", err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM region_sync_inbox WHERE event_id = $1`, ev.ID); err != nil {
		return "", err
	}
	res, err := a.applyOne(ctx, tx, ev.Origin, ev)
	if err != nil {
		return "", err
	}
	return res.Status, tx.Commit()
}
