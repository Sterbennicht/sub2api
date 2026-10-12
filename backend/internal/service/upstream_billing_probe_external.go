package service

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// 私有部署补丁：上游不是 sub2api 中转（new-api 等）时没有 /v1/sub2api/billing，
// 「上游声明倍率」永远是 unsupported。这里允许站外的对账脚本按账号写出同一格式的
// JSON（<目录>/<账号ID>.json），探测时优先读它；快照、新鲜度、倍率同步都走原流程。
//
// 文件存在即表示该账号的倍率由外部提供：读不了、格式不对或过期都记为探测失败，
// 不会回退去问上游——否则会落进 unsupported 的长退避，脚本恢复后要等很久才重探。
const (
	upstreamBillingProbeExternalDirEnv = "UPSTREAM_BILLING_PROBE_EXTERNAL_DIR"
	// observed_at 超过这个时长就当作对账脚本停了，不再把旧值当新值续期。
	upstreamBillingProbeExternalMaxAge = 2 * time.Hour
)

// probeExternalBilling 返回 handled=false 时该账号没有外部倍率，调用方照常探测上游。
func (s *UpstreamBillingProbeService) probeExternalBilling(
	ctx context.Context,
	account *Account,
	intervalMinutes int,
	now time.Time,
) (snapshot *UpstreamBillingProbeSnapshot, handled bool, err error) {
	dir := strings.TrimSpace(os.Getenv(upstreamBillingProbeExternalDirEnv))
	if dir == "" {
		return nil, false, nil
	}
	file, openErr := os.Open(filepath.Join(dir, strconv.FormatInt(account.ID, 10)+".json"))
	if errors.Is(openErr, fs.ErrNotExist) {
		return nil, false, nil
	}
	if openErr != nil {
		snapshot, err = s.persistProbeFailure(ctx, account, intervalMinutes, now, 0, "external_read_failed", 0)
		return snapshot, true, err
	}
	defer func() { _ = file.Close() }()
	body, readErr := io.ReadAll(io.LimitReader(file, upstreamBillingProbeMaxBodyBytes+1))
	if readErr != nil || len(body) > upstreamBillingProbeMaxBodyBytes {
		snapshot, err = s.persistProbeFailure(ctx, account, intervalMinutes, now, 0, "external_read_failed", 0)
		return snapshot, true, err
	}
	data, parseErr := parseUpstreamBillingProbeResponse(body)
	if parseErr != nil {
		snapshot, err = s.persistProbeFailure(ctx, account, intervalMinutes, now, 0, "external_invalid", 0)
		return snapshot, true, err
	}
	observedAt, _ := data["observed_at"].(string)
	if at, timeErr := time.Parse(time.RFC3339Nano, observedAt); timeErr != nil || now.Sub(at) > upstreamBillingProbeExternalMaxAge {
		snapshot, err = s.persistProbeFailure(ctx, account, intervalMinutes, now, 0, "external_stale", 0)
		return snapshot, true, err
	}
	// 没有发起 HTTP 请求，http_status 留 0，和真正探测到的快照区分开。
	snapshot, err = s.persistProbeSuccess(ctx, account, intervalMinutes, now, 0, data)
	return snapshot, true, err
}
