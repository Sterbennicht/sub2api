//go:build integration

package regionsync

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func containerExec(t *testing.T, script string) string {
	t.Helper()
	code, out, err := pgContainer.Exec(context.Background(), []string{"sh", "-c", script})
	require.NoError(t, err)
	b, _ := io.ReadAll(out)
	require.Zero(t, code, string(b))
	return string(b)
}

func dbName(t *testing.T, n *node) string {
	return queryString(t, n.db, `SELECT current_database()`)
}

// 初始化：GZ 先开捕获（不发送）→ 导出快照 + pg_dump → MY 恢复、清队列、错开序列 → 开始互发。
// 快照前已提交的事件不能在 MY 再应用一次；快照时还没提交的事务必须照常同步过去。
func TestBaselineSnapshotBoundary(t *testing.T) {
	my, gz := newPair(t)
	u, k, a := seedBaseline(t, gz)
	bill(t, gz, "pre-1", u, k, a, 10) // 在快照里
	ledger(t, gz, "recharge", u, "5", true)

	// 快照时仍在进行的事务。
	inflight, err := gz.db.Begin()
	require.NoError(t, err)
	_, err = inflight.Exec(`SELECT set_config('sub2api.region_sync_reason', 'admin_adjust', true)`)
	require.NoError(t, err)
	_, err = inflight.Exec(`UPDATE users SET balance = balance - 1 WHERE id = $1`, u)
	require.NoError(t, err)

	pr, pw := io.Pipe()
	outR, outW := io.Pipe()
	done := make(chan error, 1)
	c := &cli{cfg: gz.cfg, db: gz.db, out: outW, in: pr}
	go func() { done <- c.baselineSnapshot(context.Background()); _ = outW.Close() }()

	var snapID string
	sc := bufio.NewScanner(outR)
	re := regexp.MustCompile(`--snapshot=(\S+)`)
	for sc.Scan() {
		if m := re.FindStringSubmatch(sc.Text()); m != nil {
			snapID = m[1]
		}
		if strings.Contains(sc.Text(), "按回车") {
			break
		}
	}
	require.NotEmpty(t, snapID)
	go func() { _, _ = io.Copy(io.Discard, outR) }()

	// 快照导出后提交的事务、以及之后的新事件，都不在快照里。
	require.NoError(t, inflight.Commit())
	bill(t, gz, "post-1", u, k, a, 2)

	excl := ""
	for _, tbl := range baselineExcludedData {
		excl += " --exclude-table-data=" + tbl
	}
	target := fmt.Sprintf("rs_restore_%d", dbSeq.Add(1))
	containerExec(t, fmt.Sprintf(`pg_dump -U postgres -Fc --snapshot=%s%s -d %s -f /tmp/%s.dump`, snapID, excl, dbName(t, gz), target))
	_, err = pw.Write([]byte("\n"))
	require.NoError(t, err)
	require.NoError(t, <-done)
	containerExec(t, fmt.Sprintf(`createdb -U postgres %s && pg_restore -U postgres --no-owner -d %s /tmp/%s.dump`, target, target, target))

	// MY 换成恢复出来的库。
	restored, err := openRetry(strings.Replace(adminDSN, "/postgres?", "/"+target+"?", 1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = restored.Close() })
	my.db = restored
	mc := &cli{cfg: my.cfg, db: my.db, out: io.Discard}
	require.NoError(t, mc.baselineImport(context.Background(), []string{"--source", "gz", "--execute"}))
	require.NoError(t, mc.partitionSequences(context.Background(), []string{"--offset", "1", "--execute"}))
	gc := &cli{cfg: gz.cfg, db: gz.db, out: io.Discard}
	require.NoError(t, gc.partitionSequences(context.Background(), []string{"--offset", "2", "--execute"}))
	require.NoError(t, SyncLocalRow(context.Background(), my.db, my.cfg))
	my.restart(t)

	// 快照里的状态：100 - 10 + 5 = 95。
	require.Equal(t, "95.00000000", my.balance(t, u))
	require.EqualValues(t, 2, queryInt(t, gz.db, `SELECT count(*) FROM region_sync_outbox WHERE ack_status = 'baseline'`))
	require.EqualValues(t, 2, gz.pending(t), "快照之后的两条照常发送")
	exchange(t, my, gz)
	require.Equal(t, "92.00000000", my.balance(t, u))
	require.Equal(t, gz.balance(t, u), my.balance(t, u))

	// 两地新建 Key 的主键按余数错开，不会冲突。
	mustExec(t, my.db, `INSERT INTO api_keys (user_id, key, name, status) VALUES ($1, 'sk-my-new', 'm', 'active')`, u)
	mustExec(t, gz.db, `INSERT INTO api_keys (user_id, key, name, status) VALUES ($1, 'sk-gz-new', 'g', 'active')`, u)
	exchange(t, my, gz)
	require.EqualValues(t, 1, queryInt(t, my.db, `SELECT id % 10 FROM api_keys WHERE key = 'sk-my-new'`))
	require.EqualValues(t, 2, queryInt(t, gz.db, `SELECT id % 10 FROM api_keys WHERE key = 'sk-gz-new'`))
	for _, n := range []*node{my, gz} {
		require.EqualValues(t, 3, queryInt(t, n.db, `SELECT count(*) FROM api_keys`), n.id)
	}
}

func TestRollbackSQL(t *testing.T) {
	db, _ := newDB(t, "rollback")
	raw, err := os.ReadFile("../../../deploy/region-sync/rollback.sql")
	require.NoError(t, err)
	_, err = db.Exec(string(raw))
	require.NoError(t, err)
	require.EqualValues(t, 0, queryInt(t, db, `SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'trg_region_sync_%'`))
	require.EqualValues(t, 0, queryInt(t, db, `SELECT count(*) FROM pg_tables WHERE tablename LIKE 'region_sync_%'`))
	// 回滚后业务写入照常。
	_, _ = seedOne(t, db)
	bill(t, &node{db: db}, "after-rollback", 1001, 3001, 4001, 1)
}
