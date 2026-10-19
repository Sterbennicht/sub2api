package config

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/spf13/viper"
)

// RegionSyncConfig 控制 MY/GZ 异地多活的后台同步。默认关闭，关闭时行为与单节点完全一致。
//
// 每个地区只访问本地 PostgreSQL/Redis；跨区只有后台 worker 推送事件，
// 不参与鉴权、扣费、调度或启动就绪判断。
type RegionSyncConfig struct {
	Enabled bool `mapstructure:"enabled"`
	// RegionID 本地区标识，如 "my"、"gz"。
	RegionID string `mapstructure:"region_id"`
	// PrimaryRegion 管理入口/配置主写地区。
	PrimaryRegion string `mapstructure:"primary_region"`
	// PrimaryPublicURL 非主地区拦截管理写请求时提示的地址（可选）。
	PrimaryPublicURL string `mapstructure:"primary_public_url"`
	// ForwardPrimaryWrites 非主地区把「只能在主地区执行」的写请求反代到主地区，而不是直接拒绝。
	// 用于「管理入口在 MY、写主在 GZ」：MY 本地读、写转发。只有管理/注册/支付等写入会跨区，
	// 模型请求、鉴权、扣费永远只走本地。
	ForwardPrimaryWrites bool `mapstructure:"forward_primary_writes"`
	// PrimaryForwardURL 转发目标（主地区对外入口，如 https://192.0.2.10:8443）。
	PrimaryForwardURL string `mapstructure:"primary_forward_url"`
	// PrimaryForwardInsecureSkipVerify 转发目标为 IP/自签证书时开启。
	PrimaryForwardInsecureSkipVerify bool `mapstructure:"primary_forward_insecure_skip_verify"`
	// LocalAdminWritePrefixes 非主地区本地执行的管理写（只影响本机的运维类：插件、备份、运维监控等）。
	LocalAdminWritePrefixes []string `mapstructure:"local_admin_write_prefixes"`
	// UsageAggregationRegion 全局用量/账务汇总地区；其他地区把使用记录发往这里（空=主地区）。
	UsageAggregationRegion string `mapstructure:"usage_aggregation_region"`

	Peer RegionSyncPeerConfig `mapstructure:"peer"`

	// SenderEnabled 允许暂停发送（初始化基线期间先只积压不发）。
	SenderEnabled bool `mapstructure:"sender_enabled"`

	ConfigScanIntervalMS   int `mapstructure:"config_scan_interval_ms"`
	LedgerScanIntervalMS   int `mapstructure:"ledger_scan_interval_ms"`
	UsageFlushIntervalMS   int `mapstructure:"usage_flush_interval_ms"`
	BatchMaxEvents         int `mapstructure:"batch_max_events"`
	BatchMaxBytes          int `mapstructure:"batch_max_bytes"`
	RetryMinMS             int `mapstructure:"retry_min_ms"`
	RetryMaxMS             int `mapstructure:"retry_max_ms"`
	CatchupMaxBatchesPerS  int `mapstructure:"catchup_max_batches_per_second"`
	ReconcileIntervalS     int `mapstructure:"reconcile_interval_seconds"`
	ReconcileWindowHours   int `mapstructure:"reconcile_window_hours"`
	OutboxRetentionHours   int `mapstructure:"outbox_retention_hours"`
	InboxRetentionHours    int `mapstructure:"inbox_retention_hours"`
	MaxClockSkewSeconds    int `mapstructure:"max_clock_skew_seconds"`
	BacklogAlertAgeSeconds int `mapstructure:"backlog_alert_age_seconds"`

	// DefaultOAuthRefreshOwner OAuth 账号默认刷新 owner（空=主地区）。
	DefaultOAuthRefreshOwner string `mapstructure:"default_oauth_refresh_owner"`
	// LocalSettingKeys 只在本地区生效、不接收对端同步的 settings key。
	LocalSettingKeys []string `mapstructure:"local_setting_keys"`
	// ProxyURLOverrides 本地区对同步来的代理记录改用本地出口：{"代理ID": "http://127.0.0.1:7891"}；
	// 值为 "direct" 表示该代理在本地区直连。
	ProxyURLOverrides map[string]string `mapstructure:"proxy_url_overrides"`
}

// RegionSyncPeerConfig 对端地区。
type RegionSyncPeerConfig struct {
	RegionID string `mapstructure:"region_id"`
	// URL 对端根地址，如 https://192.0.2.20:8443。
	URL string `mapstructure:"url"`
	// SharedSecret 两端相同的 HMAC 密钥（建议用环境变量 REGION_SYNC_PEER_SHARED_SECRET 注入）。
	SharedSecret string `mapstructure:"shared_secret"`
	// InsecureSkipVerify 对端为 IP 证书/自签时可开（仍有 HMAC 鉴权）。
	InsecureSkipVerify bool `mapstructure:"insecure_skip_verify"`
	// ProxyURL 发往对端的出口代理（可选）。
	ProxyURL       string `mapstructure:"proxy_url"`
	TimeoutSeconds int    `mapstructure:"timeout_seconds"`
}

func setRegionSyncDefaults() {
	viper.SetDefault("region_sync.enabled", false)
	viper.SetDefault("region_sync.region_id", "")
	viper.SetDefault("region_sync.primary_region", "")
	viper.SetDefault("region_sync.primary_public_url", "")
	viper.SetDefault("region_sync.forward_primary_writes", false)
	viper.SetDefault("region_sync.primary_forward_url", "")
	viper.SetDefault("region_sync.primary_forward_insecure_skip_verify", false)
	viper.SetDefault("region_sync.usage_aggregation_region", "")
	viper.SetDefault("region_sync.default_oauth_refresh_owner", "")
	viper.SetDefault("region_sync.local_setting_keys", []string{})
	viper.SetDefault("region_sync.peer.region_id", "")
	viper.SetDefault("region_sync.peer.url", "")
	viper.SetDefault("region_sync.peer.insecure_skip_verify", false)
	viper.SetDefault("region_sync.peer.proxy_url", "")
	viper.SetDefault("region_sync.sender_enabled", true)
	viper.SetDefault("region_sync.config_scan_interval_ms", 1000)
	viper.SetDefault("region_sync.ledger_scan_interval_ms", 1000)
	viper.SetDefault("region_sync.usage_flush_interval_ms", 2000)
	viper.SetDefault("region_sync.batch_max_events", 500)
	viper.SetDefault("region_sync.batch_max_bytes", 512*1024)
	viper.SetDefault("region_sync.retry_min_ms", 1000)
	viper.SetDefault("region_sync.retry_max_ms", 30000)
	viper.SetDefault("region_sync.catchup_max_batches_per_second", 10)
	viper.SetDefault("region_sync.reconcile_interval_seconds", 300)
	viper.SetDefault("region_sync.reconcile_window_hours", 48)
	viper.SetDefault("region_sync.outbox_retention_hours", 168)
	viper.SetDefault("region_sync.inbox_retention_hours", 720)
	viper.SetDefault("region_sync.max_clock_skew_seconds", 300)
	viper.SetDefault("region_sync.backlog_alert_age_seconds", 300)
	viper.SetDefault("region_sync.peer.timeout_seconds", 15)
	viper.SetDefault("region_sync.local_admin_write_prefixes", []string{
		"/api/v1/admin/ops", "/api/v1/admin/plugins", "/api/v1/admin/backups", "/api/v1/admin/data-management",
		"/api/v1/admin/system", "/api/v1/admin/channel-monitors", "/api/v1/admin/channel-monitor-templates",
		"/api/v1/admin/channel-monitor-v2", "/api/v1/admin/scheduled-test-plans",
	})
	_ = viper.BindEnv("region_sync.peer.shared_secret", "REGION_SYNC_PEER_SHARED_SECRET")
}

func (c *RegionSyncConfig) normalize() {
	c.RegionID = strings.ToLower(strings.TrimSpace(c.RegionID))
	c.PrimaryRegion = strings.ToLower(strings.TrimSpace(c.PrimaryRegion))
	c.Peer.RegionID = strings.ToLower(strings.TrimSpace(c.Peer.RegionID))
	c.DefaultOAuthRefreshOwner = strings.ToLower(strings.TrimSpace(c.DefaultOAuthRefreshOwner))
	c.UsageAggregationRegion = strings.ToLower(strings.TrimSpace(c.UsageAggregationRegion))
	if c.UsageAggregationRegion == "" {
		c.UsageAggregationRegion = c.PrimaryRegion
	}
}

// IsUsageAggregator 本地区是否汇总全局使用记录。
func (c *RegionSyncConfig) IsUsageAggregator() bool {
	return !c.Enabled || c.RegionID == c.UsageAggregationRegion
}

// IsPrimary 本地区是否为配置主写地区。
func (c *RegionSyncConfig) IsPrimary() bool {
	return !c.Enabled || c.RegionID == c.PrimaryRegion
}

func (c *RegionSyncConfig) validate() error {
	c.normalize()
	if !c.Enabled {
		return nil
	}
	validID := func(s string) bool {
		if s == "" || len(s) > 16 {
			return false
		}
		for _, r := range s {
			if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' {
				return false
			}
		}
		return true
	}
	if !validID(c.RegionID) || !validID(c.PrimaryRegion) || !validID(c.Peer.RegionID) {
		return fmt.Errorf("region_sync: region_id/primary_region/peer.region_id must be 1-16 chars [a-z0-9-]")
	}
	if c.Peer.RegionID == c.RegionID {
		return fmt.Errorf("region_sync: peer.region_id must differ from region_id")
	}
	if c.PrimaryRegion != c.RegionID && c.PrimaryRegion != c.Peer.RegionID {
		return fmt.Errorf("region_sync: primary_region must be region_id or peer.region_id")
	}
	if c.DefaultOAuthRefreshOwner != "" && c.DefaultOAuthRefreshOwner != c.RegionID && c.DefaultOAuthRefreshOwner != c.Peer.RegionID {
		return fmt.Errorf("region_sync: default_oauth_refresh_owner must be one of the two regions")
	}
	if c.UsageAggregationRegion != c.RegionID && c.UsageAggregationRegion != c.Peer.RegionID {
		return fmt.Errorf("region_sync: usage_aggregation_region must be one of the two regions")
	}
	if c.ForwardPrimaryWrites && c.RegionID != c.PrimaryRegion {
		fu, err := url.Parse(strings.TrimSpace(c.PrimaryForwardURL))
		if err != nil || (fu.Scheme != "https" && fu.Scheme != "http") || fu.Host == "" {
			return fmt.Errorf("region_sync: primary_forward_url must be an absolute http(s) URL when forward_primary_writes is on")
		}
	}
	u, err := url.Parse(strings.TrimSpace(c.Peer.URL))
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return fmt.Errorf("region_sync: peer.url must be an absolute http(s) URL")
	}
	if len(c.Peer.SharedSecret) < 32 {
		return fmt.Errorf("region_sync: peer.shared_secret must be at least 32 characters")
	}
	if c.BatchMaxEvents <= 0 || c.BatchMaxBytes <= 0 {
		return fmt.Errorf("region_sync: batch limits must be positive")
	}
	if c.RetryMinMS <= 0 || c.RetryMaxMS < c.RetryMinMS {
		return fmt.Errorf("region_sync: retry_min_ms/retry_max_ms invalid")
	}
	return nil
}
