//go:build unit

package regionsync

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/region"
	"github.com/stretchr/testify/require"
)

// 迁移里的触发器段必须与注册表生成的 DDL 一致：改了字段白名单就要同步改迁移（新迁移）。
func TestMigrationTriggersMatchRegistry(t *testing.T) {
	raw, err := os.ReadFile("../../migrations/246_region_sync_foundation.sql")
	require.NoError(t, err)
	mig := strings.ReplaceAll(string(raw), "\r\n", "\n")
	require.Contains(t, mig, strings.ReplaceAll(TriggerDDL(), "\r\n", "\n"))
}

func TestRegistryIsConsistent(t *testing.T) {
	seen := map[string]bool{}
	for _, tbl := range ConfigTables {
		require.False(t, seen[tbl.Name], "duplicate table %s", tbl.Name)
		seen[tbl.Name] = true
		require.NotEmpty(t, tbl.PK, tbl.Name)
		cols := map[string]string{}
		for _, g := range tbl.Groups {
			require.NotEqual(t, "_row", g.Name)
			for _, c := range g.Columns {
				name := columnName(c)
				require.Empty(t, cols[name], "%s.%s in groups %s and %s", tbl.Name, name, cols[name], g.Name)
				cols[name] = g.Name
				for _, pk := range tbl.PK {
					require.NotEqual(t, pk, name, "%s: pk %s must not be in a field group", tbl.Name, pk)
				}
			}
		}
	}
	for _, d := range DeltaTables {
		cfg, ok := configTable(d.Name)
		require.True(t, ok, "delta table %s must also be a config table (row must exist)", d.Name)
		for _, c := range d.Columns {
			for _, g := range cfg.Groups {
				for _, gc := range g.Columns {
					require.NotEqual(t, c.Name, columnName(gc), "%s.%s is both delta and config", d.Name, c.Name)
				}
			}
		}
	}
	// 账户运行时额度计数不能被同步覆盖。
	acc, _ := configTable("accounts")
	found := false
	for _, g := range acc.Groups {
		for _, c := range g.Columns {
			if columnName(c) == "extra" {
				found = true
				require.ElementsMatch(t, AccountExtraLocalKeys, columnLocalKeys(c))
			}
		}
	}
	require.True(t, found)
}

func TestSignature(t *testing.T) {
	secret := strings.Repeat("s", 40)
	now := time.Unix(1_800_000_000, 0)
	body := []byte(`{"x":1}`)
	sig := signRequest(secret, "gz", "POST", eventsPath, now.Unix(), body)
	verify := func(origin, path, ts, s string, b []byte, at time.Time) error {
		return verifyRequest(secret, "gz", origin, "POST", path, ts, s, b, 5*time.Minute, at)
	}
	ts := strconv.FormatInt(now.Unix(), 10)
	require.NoError(t, verify("gz", eventsPath, ts, sig, body, now))
	require.NoError(t, verify("gz", eventsPath, ts, strings.ToUpper(sig), body, now.Add(4*time.Minute)))
	require.Error(t, verify("my", eventsPath, ts, sig, body, now), "origin must be the configured peer")
	require.Error(t, verify("gz", "/internal/region-sync/v1/reconcile", ts, sig, body, now), "path bound")
	require.Error(t, verify("gz", eventsPath, ts, sig, []byte(`{"x":2}`), now), "body bound")
	require.Error(t, verify("gz", eventsPath, ts, sig, body, now.Add(6*time.Minute)), "replay window")
	require.Error(t, verify("gz", eventsPath, "abc", sig, body, now))
	require.Error(t, verifyRequest("", "gz", "gz", "POST", eventsPath, ts, sig, body, time.Minute, now), "empty secret never verifies")
}

func TestGuardDecide(t *testing.T) {
	o := GuardOptions{Enabled: true, RegionID: "my", PrimaryRegion: "gz", LocalAdminWritePrefixes: []string{"/api/v1/admin/ops", "/api/v1/admin/backups/"}}
	cases := []struct {
		method, path string
		want         GuardDecision
	}{
		// 模型网关永远本地
		{"POST", "/v1/messages", GuardLocal},
		{"POST", "/v1/chat/completions", GuardLocal},
		{"POST", "/backend-api/codex/responses", GuardLocal},
		// 管理后台：读本地、运维写本地、其余写转发
		{"GET", "/api/v1/admin/users", GuardLocal},
		{"GET", "/api/v1/admin/dashboard/stats", GuardLocal},
		{"POST", "/api/v1/admin/users", GuardPrimary},
		{"PUT", "/api/v1/admin/accounts/:id", GuardPrimary},
		{"DELETE", "/api/v1/admin/groups/:id", GuardPrimary},
		{"POST", "/api/v1/admin/ops/alerts/:id/ack", GuardLocal},
		{"POST", "/api/v1/admin/backups", GuardLocal},
		{"POST", "/api/v1/admin/opsx", GuardPrimary},
		// 用户中心：登录与自助操作本地
		{"POST", "/api/v1/auth/login", GuardLocal},
		{"POST", "/api/v1/auth/login/2fa", GuardLocal},
		{"POST", "/api/v1/auth/refresh", GuardLocal},
		{"PUT", "/api/v1/user", GuardLocal},
		{"PUT", "/api/v1/user/password", GuardLocal},
		{"POST", "/api/v1/user/totp/enable", GuardLocal},
		{"DELETE", "/api/v1/user/passkeys/:id", GuardLocal},
		{"POST", "/api/v1/keys", GuardLocal},
		{"PUT", "/api/v1/keys/:id", GuardLocal},
		{"DELETE", "/api/v1/keys/:id", GuardLocal},
		{"GET", "/api/v1/usage", GuardLocal},
		// 只能在主地区：注册、兑换、支付、第三方登录
		{"POST", "/api/v1/auth/register", GuardPrimary},
		{"POST", "/api/v1/redeem", GuardPrimary},
		{"POST", "/api/v1/payment/orders", GuardPrimary},
		{"GET", "/api/v1/auth/oauth/linuxdo/callback", GuardPrimary},
		{"POST", "/api/v1/payment/webhook/epay", GuardPrimary},
		{"GET", "/api/v1/payment/webhook/epay", GuardPrimary},
	}
	for _, c := range cases {
		require.Equal(t, c.want, o.Decide(c.method, c.path), c.method+" "+c.path)
	}
	require.Nil(t, WriteGuard(GuardOptions{Enabled: true, RegionID: "gz", PrimaryRegion: "gz"}))
	require.Nil(t, WriteGuard(GuardOptions{Enabled: false, RegionID: "my", PrimaryRegion: "gz"}))
	require.NotNil(t, WriteGuard(o))
}

func TestRegionGates(t *testing.T) {
	region.Reset()
	t.Cleanup(region.Reset)
	// 关闭时一切照旧。
	require.True(t, region.AccountAllowedHere(map[string]any{"region_pool": "gz"}))
	require.True(t, region.IsRefreshOwner(map[string]any{"region_refresh_owner": "gz"}))
	_, ok := region.ProxyURLOverride(1)
	require.False(t, ok)

	region.Set(region.Identity{Enabled: true, RegionID: "my", PrimaryRegion: "gz", DefaultOAuthOwner: "gz",
		ProxyURLOverrides: map[int64]string{7: "http://127.0.0.1:7891"}})
	require.True(t, region.AccountAllowedHere(nil))
	require.True(t, region.AccountAllowedHere(map[string]any{"region_pool": "my"}))
	require.True(t, region.AccountAllowedHere(map[string]any{"region_pool": []any{"gz", "my"}}))
	require.False(t, region.AccountAllowedHere(map[string]any{"region_pool": "gz"}))
	// 刷新 owner：显式 > 单地区池 > 默认。
	require.False(t, region.IsRefreshOwner(nil))
	require.True(t, region.IsRefreshOwner(map[string]any{"region_pool": "my"}))
	require.True(t, region.IsRefreshOwner(map[string]any{"region_refresh_owner": "my", "region_pool": "gz"}))
	require.False(t, region.IsRefreshOwner(map[string]any{"region_pool": []any{"gz", "my"}}))
	u, ok := region.ProxyURLOverride(7)
	require.True(t, ok)
	require.Equal(t, "http://127.0.0.1:7891", u)

	// 配置里写 direct：该代理在本地区直连（URL 为空）。
	PublishIdentity(config.RegionSyncConfig{Enabled: true, RegionID: "my", PrimaryRegion: "gz",
		ProxyURLOverrides: map[string]string{"3": "Direct", "5": "http://127.0.0.1:7892", "x": "direct", "9": " "}})
	u, ok = region.ProxyURLOverride(3)
	require.True(t, ok)
	require.Empty(t, u)
	u, _ = region.ProxyURLOverride(5)
	require.Equal(t, "http://127.0.0.1:7892", u)
	_, ok = region.ProxyURLOverride(9)
	require.False(t, ok)
}
