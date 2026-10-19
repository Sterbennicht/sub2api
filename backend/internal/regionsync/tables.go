package regionsync

import (
	"fmt"
	"strings"
)

// Owner 决定一个字段组由哪些地区发出、接收端接受谁的版本。
type Owner string

const (
	// OwnerPrimary 管理字段：只有主地区（MY）发出；接收端只接受来自主地区的版本。
	OwnerPrimary Owner = "primary"
	// OwnerAny 用户自助字段：两地都可本地修改，按 (HLC 时间戳, 地区) 最后写入胜出。
	OwnerAny Owner = "any"
	// OwnerInsert 只在接收端新建行时写入（如 created_at），之后不再覆盖。
	OwnerInsert Owner = "insert"
)

// FieldGroup 一组一起版本化的列。列名后缀 "*k1|k2" 表示 jsonb 列，
// 其中 k1、k2 是本地运行状态键：发出时剔除，接收时保留本地值。
type FieldGroup struct {
	Name    string
	Owner   Owner
	Columns []string
}

// ConfigTable 走「字段白名单 + 字段组版本 + 删除墓碑」同步的表。
// 未列在任何组里的列（余额、计数器、运行状态）永远不会被同步覆盖。
type ConfigTable struct {
	Name   string
	PK     []string
	Groups []FieldGroup
	// DeleteOwner 谁可以发出硬删除。
	DeleteOwner Owner
	// StickyDeleteColumn 软删除列：本地一旦非空，任何版本的事件都不能把它改回空（Key 删除不可复活）。
	StickyDeleteColumn string
	// AcceptValues 列 -> 接收端允许写入的值；其他值丢弃、保留本地值（账号 error 状态是本地运行状态）。
	AcceptValues map[string][]string
	// OmitIDOnInsert 主键以外还有自增 id 但按业务键同步的表（settings 按 key）。
	OmitIDOnInsert bool
}

// DeltaColumn 计数类列：只同步实际发生的增量，不覆盖对端整行。
type DeltaColumn struct {
	Name string
	// WindowColumn 非空时：窗口起点变化视为窗口重置，增量取新值本身。
	WindowColumn string
	// ZeroIsReset 新值为 0 且旧值非 0 时视为重置（如手动重置 Key 已用额度）。
	ZeroIsReset bool
}

// DeltaTable 走增量同步的表。
type DeltaTable struct {
	Name    string
	PK      []string
	Columns []DeltaColumn
}

// AccountExtraLocalKeys accounts.extra 中按地区独立维护的运行状态键。
var AccountExtraLocalKeys = []string{
	"quota_used", "quota_daily_used", "quota_daily_start", "quota_daily_reset_at",
	"quota_weekly_used", "quota_weekly_start", "quota_weekly_reset_at",
	"model_rate_limits",
}

func patchCol(name string, localKeys []string) string {
	return name + "*" + strings.Join(localKeys, "|")
}

func cols(s string) []string { return strings.Split(s, ",") }

// ConfigTables 同步表注册表。迁移 246 的触发器参数由它生成（见 TriggerArgs），
// 集成测试会校验数据库里的触发器参数与这里一致。
var ConfigTables = []ConfigTable{
	{
		Name: "users", PK: []string{"id"}, DeleteOwner: OwnerPrimary,
		Groups: []FieldGroup{
			{"admin", OwnerPrimary, cols("role,concurrency,status,notes,rpm_limit,restrict_public_groups,signup_source,wechat,deleted_at")},
			{"profile", OwnerAny, cols("username,balance_notify_enabled,balance_notify_threshold,balance_notify_threshold_type,balance_notify_extra_emails")},
			{"cred", OwnerAny, cols("email,password_hash,totp_secret_encrypted,totp_enabled,totp_enabled_at")},
			{"meta", OwnerInsert, cols("created_at")},
		},
	},
	{
		Name: "api_keys", PK: []string{"id"}, DeleteOwner: OwnerAny, StickyDeleteColumn: "deleted_at",
		Groups: []FieldGroup{
			{"ident", OwnerAny, cols("user_id,key")},
			{"cfg", OwnerAny, cols("name,group_id,status,ip_whitelist,ip_blacklist,quota,expires_at,rate_limit_5h,rate_limit_1d,rate_limit_7d")},
			{"life", OwnerAny, cols("deleted_at")},
			{"meta", OwnerInsert, cols("created_at")},
		},
	},
	{
		Name: "groups", PK: []string{"id"}, DeleteOwner: OwnerPrimary,
		Groups: []FieldGroup{
			{"cfg", OwnerPrimary, cols("name,description,rate_multiplier,is_exclusive,status,deleted_at,platform,subscription_type,daily_limit_usd,weekly_limit_usd,monthly_limit_usd,default_validity_days,image_price_1k,image_price_2k,image_price_4k,claude_code_only,fallback_group_id,model_routing,model_routing_enabled,fallback_group_id_on_invalid_request,mcp_xml_inject,supported_model_scopes,sort_order,allow_messages_dispatch,default_mapped_model,require_oauth_only,require_privacy_set,messages_dispatch_model_config,rpm_limit,allow_image_generation,image_rate_independent,image_rate_multiplier,model_allowlist,peak_rate_enabled,peak_start,peak_end,peak_rate_multiplier,batch_image_discount_multiplier,batch_image_hold_multiplier,allow_batch_image_generation,video_rate_independent,video_rate_multiplier,video_price_480p,video_price_720p,video_price_1080p,web_search_price_per_call,duplicate_operation_id,max_reasoning_effort,reasoning_effort_mappings,allow_live,profit_control_enabled,profit_min_margin,profit_safety_buffer,video_model_prices,audio_realtime_price_per_min,audio_tts_price_per_million_chars,audio_stt_price_per_hour,search_price_per_1k,long_context_pricing_enabled,model_pricing,force_openai_fast,max_reasoning_effort_over_limit,free_openai_fast,codex_models_manifest_config,i18n")},
			{"meta", OwnerInsert, cols("created_at")},
		},
	},
	{
		Name: "user_allowed_groups", PK: []string{"user_id", "group_id"}, DeleteOwner: OwnerPrimary,
		Groups: []FieldGroup{{"meta", OwnerPrimary, cols("created_at")}},
	},
	{
		Name: "user_group_rate_multipliers", PK: []string{"user_id", "group_id"}, DeleteOwner: OwnerPrimary,
		Groups: []FieldGroup{
			{"cfg", OwnerPrimary, cols("rate_multiplier,rpm_override")},
			{"meta", OwnerInsert, cols("created_at")},
		},
	},
	{
		Name: "user_subscriptions", PK: []string{"id"}, DeleteOwner: OwnerPrimary,
		Groups: []FieldGroup{
			{"cfg", OwnerPrimary, cols("user_id,group_id,starts_at,expires_at,status,assigned_by,assigned_at,notes,deleted_at")},
			{"meta", OwnerInsert, cols("created_at,daily_window_start,weekly_window_start,monthly_window_start")},
		},
	},
	{
		Name: "user_platform_quotas", PK: []string{"id"}, DeleteOwner: OwnerPrimary,
		Groups: []FieldGroup{
			{"cfg", OwnerPrimary, cols("user_id,platform,daily_limit_usd,weekly_limit_usd,monthly_limit_usd,deleted_at")},
			{"meta", OwnerInsert, cols("created_at")},
		},
	},
	{
		Name: "accounts", PK: []string{"id"}, DeleteOwner: OwnerPrimary,
		AcceptValues: map[string][]string{"status": {"active", "disabled"}},
		Groups: []FieldGroup{
			{"cfg", OwnerPrimary, cols("name,notes,platform,type,proxy_id,proxy_fallback_origin_id,concurrency,load_factor,priority,rate_multiplier,status,schedulable,expires_at,auto_pause_on_expired,parent_account_id,quota_dimension,deleted_at")},
			{"extra", OwnerPrimary, []string{patchCol("extra", AccountExtraLocalKeys)}},
			// 凭据：任何地区（刷新 owner）都可写，按 HLC 取最新，旧配置不能覆盖新 token。
			{"cred", OwnerAny, cols("credentials")},
			{"meta", OwnerInsert, cols("created_at")},
		},
	},
	{
		Name: "account_groups", PK: []string{"account_id", "group_id"}, DeleteOwner: OwnerPrimary,
		Groups: []FieldGroup{
			{"cfg", OwnerPrimary, cols("priority")},
			{"meta", OwnerInsert, cols("created_at")},
		},
	},
	{
		Name: "proxies", PK: []string{"id"}, DeleteOwner: OwnerPrimary,
		Groups: []FieldGroup{
			{"cfg", OwnerPrimary, cols("name,protocol,host,port,username,password,status,expires_at,fallback_mode,backup_proxy_id,expiry_warn_days,deleted_at")},
			{"meta", OwnerInsert, cols("created_at")},
		},
	},
	{
		Name: "channels", PK: []string{"id"}, DeleteOwner: OwnerPrimary,
		Groups: []FieldGroup{
			{"cfg", OwnerPrimary, cols("name,description,status,model_mapping,billing_model_source,restrict_models,features,apply_pricing_to_account_stats,features_config")},
			{"meta", OwnerInsert, cols("created_at")},
		},
	},
	{
		Name: "channel_groups", PK: []string{"id"}, DeleteOwner: OwnerPrimary,
		Groups: []FieldGroup{
			{"cfg", OwnerPrimary, cols("channel_id,group_id")},
			{"meta", OwnerInsert, cols("created_at")},
		},
	},
	{
		Name: "channel_model_pricing", PK: []string{"id"}, DeleteOwner: OwnerPrimary,
		Groups: []FieldGroup{
			{"cfg", OwnerPrimary, cols("channel_id,models,input_price,output_price,cache_write_price,cache_read_price,image_output_price,billing_mode,per_request_price,platform,image_input_price,time_pricing,fast_multiplier,flex_multiplier,cache_write_1h_price,max_reasoning_effort_multiplier,reasoning_effort_multipliers")},
			{"meta", OwnerInsert, cols("created_at")},
		},
	},
	{
		Name: "channel_pricing_intervals", PK: []string{"id"}, DeleteOwner: OwnerPrimary,
		Groups: []FieldGroup{
			{"cfg", OwnerPrimary, cols("pricing_id,min_tokens,max_tokens,tier_label,input_price,output_price,cache_write_price,cache_read_price,per_request_price,sort_order,input_multiplier,output_multiplier,cache_write_multiplier,cache_read_multiplier,cache_write_1h_price")},
			{"meta", OwnerInsert, cols("created_at")},
		},
	},
	{
		Name: "composite_model_routes", PK: []string{"id"}, DeleteOwner: OwnerPrimary,
		Groups: []FieldGroup{
			{"cfg", OwnerPrimary, cols("group_id,public_model,match_type,target_platform,upstream_model,endpoint,priority,enabled,notes,deleted_at")},
			{"meta", OwnerInsert, cols("created_at")},
		},
	},
	{
		Name: "error_passthrough_rules", PK: []string{"id"}, DeleteOwner: OwnerPrimary,
		Groups: []FieldGroup{
			{"cfg", OwnerPrimary, cols("name,enabled,priority,error_codes,keywords,match_mode,platforms,passthrough_code,response_code,passthrough_body,custom_message,description,skip_monitoring")},
			{"meta", OwnerInsert, cols("created_at")},
		},
	},
	{
		Name: "tls_fingerprint_profiles", PK: []string{"id"}, DeleteOwner: OwnerPrimary,
		Groups: []FieldGroup{
			{"cfg", OwnerPrimary, cols("name,description,enable_grease,cipher_suites,curves,point_formats,signature_algorithms,alpn_protocols,supported_versions,key_share_groups,psk_modes,extensions")},
			{"meta", OwnerInsert, cols("created_at")},
		},
	},
	{
		Name: "settings", PK: []string{"key"}, DeleteOwner: OwnerPrimary, OmitIDOnInsert: true,
		Groups: []FieldGroup{{"cfg", OwnerPrimary, cols("value")}},
	},
	{
		Name: "passkey_credentials", PK: []string{"id"}, DeleteOwner: OwnerAny,
		Groups: []FieldGroup{
			{"cfg", OwnerAny, cols("user_id,credential_id,name,credential_data")},
			{"meta", OwnerInsert, cols("created_at")},
		},
	},
	{
		Name: "passkey_user_handles", PK: []string{"user_id"}, DeleteOwner: OwnerAny,
		Groups: []FieldGroup{
			{"cfg", OwnerAny, cols("user_handle")},
			{"meta", OwnerInsert, cols("created_at")},
		},
	},
}

// DeltaTables 增量同步的计数列。
var DeltaTables = []DeltaTable{
	{Name: "users", PK: []string{"id"}, Columns: []DeltaColumn{{Name: "balance"}, {Name: "frozen_balance"}, {Name: "total_recharged"}}},
	{Name: "api_keys", PK: []string{"id"}, Columns: []DeltaColumn{{Name: "quota_used", ZeroIsReset: true}}},
	{Name: "user_subscriptions", PK: []string{"id"}, Columns: []DeltaColumn{
		{Name: "daily_usage_usd", WindowColumn: "daily_window_start"},
		{Name: "weekly_usage_usd", WindowColumn: "weekly_window_start"},
		{Name: "monthly_usage_usd", WindowColumn: "monthly_window_start"},
	}},
}

// IDPartitionedTables 两地都会本地插入、需要按地区错开主键序列的表。
var IDPartitionedTables = []string{"users", "api_keys", "passkey_credentials"}

func configTable(name string) (*ConfigTable, bool) {
	for i := range ConfigTables {
		if ConfigTables[i].Name == name {
			return &ConfigTables[i], true
		}
	}
	return nil, false
}

func deltaTable(name string) (*DeltaTable, bool) {
	for i := range DeltaTables {
		if DeltaTables[i].Name == name {
			return &DeltaTables[i], true
		}
	}
	return nil, false
}

// columnName 去掉 jsonb 本地键后缀。
func columnName(spec string) string {
	if i := strings.IndexByte(spec, '*'); i >= 0 {
		return spec[:i]
	}
	return spec
}

// columnLocalKeys 返回 jsonb 列的本地键。
func columnLocalKeys(spec string) []string {
	i := strings.IndexByte(spec, '*')
	if i < 0 || i == len(spec)-1 {
		return nil
	}
	return strings.Split(spec[i+1:], "|")
}

// TriggerArgs 生成 region_sync_capture_config 的触发器参数。
func (t ConfigTable) TriggerArgs() []string {
	args := []string{strings.Join(t.PK, ","), "del=" + string(t.DeleteOwner)}
	for _, g := range t.Groups {
		args = append(args, fmt.Sprintf("%s:%s:%s", g.Name, g.Owner, strings.Join(g.Columns, ",")))
	}
	return args
}

// TriggerArgs 生成 region_sync_capture_delta 的触发器参数。
func (t DeltaTable) TriggerArgs() []string {
	args := []string{strings.Join(t.PK, ",")}
	for _, c := range t.Columns {
		spec := c.Name
		if c.WindowColumn != "" {
			spec += "@" + c.WindowColumn
		}
		if c.ZeroIsReset {
			spec += "!"
		}
		args = append(args, spec)
	}
	return args
}

// quoteIdent 只用于注册表里的固定标识符。
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// LocalColumns 明确按地区独立维护、不参与同步的列（运行状态、本地计数、本地时间戳）。
// 不在这里、也不在同步字段/增量列里的列会在启动时告警，提示升级后补齐归类。
var LocalColumns = map[string][]string{
	"users":                {"last_login_at", "last_active_at"},
	"api_keys":             {"last_used_at", "usage_5h", "usage_1d", "usage_7d", "window_5h_start", "window_1d_start", "window_7d_start"},
	"user_platform_quotas": {"daily_usage_usd", "weekly_usage_usd", "monthly_usage_usd", "daily_window_start", "weekly_window_start", "monthly_window_start"},
	"accounts": {"error_message", "last_used_at", "rate_limited_at", "rate_limit_reset_at", "overload_until",
		"temp_unschedulable_until", "temp_unschedulable_reason", "session_window_start", "session_window_end", "session_window_status"},
	"passkey_credentials": {"last_used_at"},
	"settings":            {"id"},
}
