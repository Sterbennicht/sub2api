package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newExternalBillingProbeFixture(t *testing.T) (*Account, *httpUpstreamRecorder, *UpstreamBillingProbeService, string, time.Time) {
	t.Helper()
	initialRate := 0.35
	account := &Account{
		ID:          84,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Status:      StatusActive,
		Concurrency: 1,
		Credentials: map[string]any{"api_key": "sk-test", "base_url": "https://newapi.example"},
		Extra: map[string]any{
			UpstreamBillingProbeEnabledExtraKey:    true,
			UpstreamBillingRateSyncEnabledExtraKey: true,
		},
		RateMultiplier: &initialRate,
	}
	repo := &upstreamBillingProbeAccountRepo{accounts: map[int64]*Account{account.ID: account}}
	// new-api 上游没有这个端点：真去探测只会得到 404。
	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusNotFound,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader("not found")),
	}}
	svc := newUpstreamBillingProbeTestService(repo, upstream, &upstreamBillingProbeSettingRepo{})
	fixedNow := time.Date(2026, time.October, 12, 6, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return fixedNow }
	dir := t.TempDir()
	t.Setenv(upstreamBillingProbeExternalDirEnv, dir)
	return account, upstream, svc, dir, fixedNow
}

func writeExternalBilling(t *testing.T, dir string, accountID int64, rate float64, observedAt time.Time) {
	t.Helper()
	body := fmt.Sprintf(`{"object":"sub2api.key_billing","schema_version":1,"billing_scope":"token",
		"group_rate_multiplier":%[1]v,"resolved_rate_multiplier":%[1]v,"peak_rate_enabled":false,
		"effective_rate_multiplier":%[1]v,"observed_at":%[2]q}`, rate, observedAt.Format(time.RFC3339))
	require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("%d.json", accountID)), []byte(body), 0o644))
}

func TestUpstreamBillingProbeUsesExternalRateWithoutCallingUpstream(t *testing.T) {
	account, upstream, svc, dir, fixedNow := newExternalBillingProbeFixture(t)
	writeExternalBilling(t, dir, account.ID, 0.253, fixedNow.Add(-10*time.Minute))

	snapshot, err := svc.ProbeAccount(context.Background(), account.ID)
	require.NoError(t, err)
	require.Equal(t, UpstreamBillingProbeStatusOK, snapshot.Status)
	require.Equal(t, 0.253, snapshot.Data["resolved_rate_multiplier"])
	require.Equal(t, fixedNow, *snapshot.ReceivedAt)
	require.Equal(t, fixedNow.Add(time.Hour), *snapshot.FreshUntil)
	require.Zero(t, snapshot.HTTPStatus)
	// 上游的 Key 不能被带去任何地方：外部倍率只读本地文件。
	require.Nil(t, upstream.lastReq)
	require.Equal(t, 0.253, *account.RateMultiplier)
	require.Equal(t, 0.253, *snapshot.SyncedRateMultiplier)
}

func TestUpstreamBillingProbeWithoutExternalFileProbesUpstream(t *testing.T) {
	account, upstream, svc, dir, fixedNow := newExternalBillingProbeFixture(t)
	writeExternalBilling(t, dir, account.ID+1, 0.253, fixedNow)

	snapshot, err := svc.ProbeAccount(context.Background(), account.ID)
	require.NoError(t, err)
	require.Equal(t, UpstreamBillingProbeStatusUnsupported, snapshot.Status)
	require.NotNil(t, upstream.lastReq)
	require.Equal(t, 0.35, *account.RateMultiplier)
}

func TestUpstreamBillingProbeExternalRateFailsClosed(t *testing.T) {
	cases := map[string]func(t *testing.T, dir string, account *Account, now time.Time){
		"external_stale": func(t *testing.T, dir string, account *Account, now time.Time) {
			writeExternalBilling(t, dir, account.ID, 0.253, now.Add(-upstreamBillingProbeExternalMaxAge-time.Minute))
		},
		"external_invalid": func(t *testing.T, dir string, account *Account, _ time.Time) {
			path := filepath.Join(dir, fmt.Sprintf("%d.json", account.ID))
			require.NoError(t, os.WriteFile(path, []byte(`{"resolved_rate_multiplier":0.253}`), 0o644))
		},
		"external_read_failed": func(t *testing.T, dir string, account *Account, _ time.Time) {
			require.NoError(t, os.Mkdir(filepath.Join(dir, fmt.Sprintf("%d.json", account.ID)), 0o755))
		},
	}
	for reason, prepare := range cases {
		t.Run(reason, func(t *testing.T) {
			account, upstream, svc, dir, fixedNow := newExternalBillingProbeFixture(t)
			prepare(t, dir, account, fixedNow)

			snapshot, err := svc.ProbeAccount(context.Background(), account.ID)
			require.NoError(t, err)
			// 记成普通失败而不是 unsupported：按常规间隔重试，脚本恢复后很快能接上。
			require.Equal(t, UpstreamBillingProbeStatusFailed, snapshot.Status)
			require.Equal(t, reason, snapshot.LastError)
			require.False(t, snapshot.NextProbeAt.After(fixedNow.Add(36*time.Minute)))
			require.Nil(t, upstream.lastReq)
			require.Equal(t, 0.35, *account.RateMultiplier)
		})
	}
}
