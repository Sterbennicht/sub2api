-- MY/GZ 异地多活：本地 outbox / inbox / 字段组版本 / 捕获触发器。
--
-- 设计要点（详见 deploy/region-sync/README.md）：
-- * 所有捕获都在业务事务内由触发器完成：业务提交 = 事件持久化，回滚 = 事件消失。
-- * region_sync_local 没有启用行时，触发器读一行小表即返回，单节点行为不变。
-- * 接收端写库时设置 sub2api.region_sync_applying=on，捕获触发器跳过，避免同步回环；
--   已有的鉴权缓存失效触发器照常工作。
-- * 配置类按「字段组 + HLC 版本」同步，余额/额度类只发实际增量，二者互不覆盖。

CREATE TABLE IF NOT EXISTS region_sync_local (
    id          SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    region_id   TEXT NOT NULL,
    is_primary  BOOLEAN NOT NULL DEFAULT FALSE,
    enabled     BOOLEAN NOT NULL DEFAULT FALSE,
    ship_usage  BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- stream: 1=配置/Key 状态, 2=充值/退款/调账/冻结（高优先级）, 3=消费增量与使用记录（批量）
CREATE TABLE IF NOT EXISTS region_sync_outbox (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    origin_region   TEXT NOT NULL,
    schema_version  SMALLINT NOT NULL DEFAULT 1,
    stream          SMALLINT NOT NULL CHECK (stream IN (1, 2, 3)),
    entity_type     TEXT NOT NULL,
    entity_key      TEXT NOT NULL,
    payload         JSONB NOT NULL,
    txid            XID8 NOT NULL DEFAULT pg_current_xact_id(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    acked_at        TIMESTAMPTZ,
    ack_status      TEXT,
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT
);
CREATE INDEX IF NOT EXISTS idx_region_sync_outbox_pending
    ON region_sync_outbox (stream, id) WHERE acked_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_region_sync_outbox_acked
    ON region_sync_outbox (acked_at) WHERE acked_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS region_sync_inbox (
    event_id       UUID PRIMARY KEY,
    origin_region  TEXT NOT NULL,
    origin_seq     BIGINT NOT NULL,
    stream         SMALLINT NOT NULL,
    status         TEXT NOT NULL,
    received_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_region_sync_inbox_origin_seq
    ON region_sync_inbox (origin_region, origin_seq);
CREATE INDEX IF NOT EXISTS idx_region_sync_inbox_received
    ON region_sync_inbox (received_at);

-- 每个 (实体, 字段组) 的最新版本。field_group='_row' 是硬删除墓碑。
CREATE TABLE IF NOT EXISTS region_sync_versions (
    entity_type  TEXT NOT NULL,
    entity_key   TEXT NOT NULL,
    field_group  TEXT NOT NULL,
    v_ts         BIGINT NOT NULL,
    v_region     TEXT NOT NULL,
    PRIMARY KEY (entity_type, entity_key, field_group)
);

-- 暂时无法落地的事件：增量对应的行尚未同步到、使用记录外键未就绪、或被拒绝待人工处理。
CREATE TABLE IF NOT EXISTS region_sync_deferred (
    id            BIGSERIAL PRIMARY KEY,
    event_id      UUID NOT NULL,
    kind          TEXT NOT NULL CHECK (kind IN ('delta', 'usage', 'rejected')),
    entity_type   TEXT NOT NULL,
    entity_key    TEXT NOT NULL,
    origin_region TEXT NOT NULL,
    payload       JSONB NOT NULL,
    attempts      INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (event_id)
);
CREATE INDEX IF NOT EXISTS idx_region_sync_deferred_entity
    ON region_sync_deferred (entity_type, entity_key);

-- 同步落库后要做的本地缓存失效，与落库同事务写入，提交后由 worker 执行（崩溃后重启续做）。
CREATE TABLE IF NOT EXISTS region_sync_pending_invalidations (
    id          BIGSERIAL PRIMARY KEY,
    kind        TEXT NOT NULL,
    ref         TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 跨区导入的使用记录：来源地区的 id -> 本地 id，避免两地 id 冲突、重复导入。
CREATE TABLE IF NOT EXISTS region_sync_usage_import (
    origin_region TEXT NOT NULL,
    origin_id     BIGINT NOT NULL,
    local_id      BIGINT NOT NULL,
    imported_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (origin_region, origin_id)
);

-- 初始化基线：快照里已经包含的对端事件按 txid 可见性一次性标记，之后不再重放。
CREATE TABLE IF NOT EXISTS region_sync_baseline (
    id            BIGSERIAL PRIMARY KEY,
    source_region TEXT NOT NULL,
    snapshot      pg_snapshot NOT NULL,
    note          TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION region_sync_applying()
RETURNS BOOLEAN
LANGUAGE sql
STABLE
AS $$
    SELECT COALESCE(current_setting('sub2api.region_sync_applying', true), '') = 'on'
$$;

-- 取 jsonb 的部分列；"col*k1|k2" 表示 jsonb 列并去掉本地运行状态键。
CREATE OR REPLACE FUNCTION region_sync_project(j JSONB, cols TEXT[])
RETURNS JSONB
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    result JSONB := '{}'::jsonb;
    c TEXT;
    col_name TEXT;
    excluded TEXT[];
    v JSONB;
BEGIN
    IF j IS NULL THEN
        RETURN NULL;
    END IF;
    FOREACH c IN ARRAY cols LOOP
        IF position('*' IN c) > 0 THEN
            col_name := split_part(c, '*', 1);
            excluded := string_to_array(split_part(c, '*', 2), '|');
            v := j -> col_name;
            IF v IS NOT NULL AND jsonb_typeof(v) = 'object' AND excluded IS NOT NULL THEN
                v := v - excluded;
            END IF;
        ELSE
            col_name := c;
            v := j -> c;
        END IF;
        result := result || jsonb_build_object(col_name, COALESCE(v, 'null'::jsonb));
    END LOOP;
    RETURN result;
END;
$$;

CREATE OR REPLACE FUNCTION region_sync_entity_key(j JSONB, pk_cols TEXT[])
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT string_agg(j ->> c, '|' ORDER BY ord)
    FROM unnest(pk_cols) WITH ORDINALITY AS u(c, ord)
$$;

-- HLC：取 max(当前微秒, 已存版本 + 1)，保证同一实体本地版本单调递增。
CREATE OR REPLACE FUNCTION region_sync_bump_version(p_type TEXT, p_key TEXT, p_group TEXT, p_region TEXT)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    now_us BIGINT := floor(extract(epoch FROM clock_timestamp()) * 1000000)::BIGINT;
    ts BIGINT;
BEGIN
    INSERT INTO region_sync_versions AS v (entity_type, entity_key, field_group, v_ts, v_region)
    VALUES (p_type, p_key, p_group, now_us, p_region)
    ON CONFLICT (entity_type, entity_key, field_group)
    DO UPDATE SET v_ts = GREATEST(EXCLUDED.v_ts, v.v_ts + 1), v_region = EXCLUDED.v_region
    RETURNING v.v_ts INTO ts;
    RETURN ts;
END;
$$;

-- 配置捕获。TG_ARGV[0]=主键列, TG_ARGV[1]='del=<owner>', 其余每项 'group:owner:col,col'。
CREATE OR REPLACE FUNCTION region_sync_capture_config()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    cfg region_sync_local%ROWTYPE;
    pk_cols TEXT[] := string_to_array(TG_ARGV[0], ',');
    del_owner TEXT := split_part(TG_ARGV[1], '=', 2);
    row_j JSONB;
    old_j JSONB;
    pk_j JSONB := '{}'::jsonb;
    ekey TEXT;
    c TEXT;
    i INT;
    spec TEXT[];
    g_name TEXT;
    g_owner TEXT;
    g_cols TEXT[];
    g_new JSONB;
    changed BOOLEAN;
    may_emit BOOLEAN;
    emit BOOLEAN := FALSE;
    groups JSONB := '{}'::jsonb;
    cur_ts BIGINT;
    cur_reg TEXT;
    v_ts BIGINT;
    v_reg TEXT;
BEGIN
    IF region_sync_applying() THEN
        RETURN NULL;
    END IF;
    SELECT * INTO cfg FROM region_sync_local WHERE id = 1;
    IF NOT FOUND OR NOT cfg.enabled THEN
        RETURN NULL;
    END IF;

    IF TG_OP = 'DELETE' THEN
        row_j := to_jsonb(OLD);
    ELSE
        row_j := to_jsonb(NEW);
    END IF;
    IF TG_OP = 'UPDATE' THEN
        old_j := to_jsonb(OLD);
    END IF;
    FOREACH c IN ARRAY pk_cols LOOP
        pk_j := pk_j || jsonb_build_object(c, row_j -> c);
    END LOOP;
    ekey := region_sync_entity_key(row_j, pk_cols);

    IF TG_OP = 'DELETE' THEN
        IF del_owner = 'primary' AND NOT cfg.is_primary THEN
            RETURN NULL;
        END IF;
        SELECT max(v.v_ts) INTO cur_ts FROM region_sync_versions v
        WHERE v.entity_type = TG_TABLE_NAME AND v.entity_key = ekey;
        INSERT INTO region_sync_versions AS v (entity_type, entity_key, field_group, v_ts, v_region)
        VALUES (TG_TABLE_NAME, ekey, '_row',
                GREATEST(floor(extract(epoch FROM clock_timestamp()) * 1000000)::BIGINT, COALESCE(cur_ts, 0) + 1),
                cfg.region_id)
        ON CONFLICT (entity_type, entity_key, field_group)
        DO UPDATE SET v_ts = EXCLUDED.v_ts, v_region = EXCLUDED.v_region
        RETURNING v.v_ts INTO v_ts;
        INSERT INTO region_sync_outbox (origin_region, stream, entity_type, entity_key, payload)
        VALUES (cfg.region_id, 1, TG_TABLE_NAME, ekey,
                jsonb_build_object('op', 'delete', 'pk', pk_j, 'v', v_ts, 'r', cfg.region_id));
        PERFORM pg_notify('region_sync_wake', '1');
        RETURN NULL;
    END IF;

    FOR i IN 2 .. TG_NARGS - 1 LOOP
        spec := regexp_match(TG_ARGV[i], '^([^:]+):([^:]+):(.*)$');
        g_name := spec[1];
        g_owner := spec[2];
        g_cols := string_to_array(spec[3], ',');
        g_new := region_sync_project(row_j, g_cols);
        changed := TG_OP = 'INSERT' OR g_new IS DISTINCT FROM region_sync_project(old_j, g_cols);
        may_emit := g_owner = 'any' OR (g_owner = 'primary' AND cfg.is_primary);

        SELECT v.v_ts, v.v_region INTO cur_ts, cur_reg FROM region_sync_versions v
        WHERE v.entity_type = TG_TABLE_NAME AND v.entity_key = ekey AND v.field_group = g_name;

        IF changed AND may_emit THEN
            v_ts := region_sync_bump_version(TG_TABLE_NAME, ekey, g_name, cfg.region_id);
            v_reg := cfg.region_id;
            emit := TRUE;
        ELSE
            v_ts := COALESCE(cur_ts, 0);
            v_reg := COALESCE(cur_reg, '');
        END IF;
        groups := groups || jsonb_build_object(g_name, jsonb_build_object('v', v_ts, 'r', v_reg, 'c', g_new));
    END LOOP;

    IF NOT emit THEN
        RETURN NULL;
    END IF;
    INSERT INTO region_sync_outbox (origin_region, stream, entity_type, entity_key, payload)
    VALUES (cfg.region_id, 1, TG_TABLE_NAME, ekey,
            jsonb_build_object('op', 'upsert', 'pk', pk_j, 'g', groups));
    PERFORM pg_notify('region_sync_wake', '1');
    RETURN NULL;
END;
$$;

-- 增量捕获。TG_ARGV[0]=主键列，其余为 'col' / 'col@window_col' / 'col!'（0 视为重置）。
-- 金额用 numeric 文本传输，精度与 DECIMAL(20,8) 一致，接收端不重算价格。
CREATE OR REPLACE FUNCTION region_sync_capture_delta()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    cfg region_sync_local%ROWTYPE;
    pk_cols TEXT[] := string_to_array(TG_ARGV[0], ',');
    new_j JSONB := to_jsonb(NEW);
    old_j JSONB;
    pk_j JSONB := '{}'::jsonb;
    deltas JSONB := '{}'::jsonb;
    resets JSONB := '[]'::jsonb;
    c TEXT;
    i INT;
    spec TEXT;
    col TEXT;
    win TEXT;
    zero_reset BOOLEAN;
    new_v NUMERIC;
    old_v NUMERIC;
    d NUMERIC;
    reason TEXT;
    stream_no SMALLINT;
BEGIN
    IF region_sync_applying() THEN
        RETURN NULL;
    END IF;
    SELECT * INTO cfg FROM region_sync_local WHERE id = 1;
    IF NOT FOUND OR NOT cfg.enabled THEN
        RETURN NULL;
    END IF;
    IF TG_OP = 'UPDATE' THEN
        old_j := to_jsonb(OLD);
    END IF;

    FOR i IN 1 .. TG_NARGS - 1 LOOP
        spec := TG_ARGV[i];
        zero_reset := right(spec, 1) = '!';
        IF zero_reset THEN
            spec := left(spec, length(spec) - 1);
        END IF;
        col := split_part(spec, '@', 1);
        win := NULLIF(split_part(spec, '@', 2), '');
        new_v := COALESCE((new_j ->> col)::NUMERIC, 0);
        old_v := COALESCE((old_j ->> col)::NUMERIC, 0);
        IF win IS NOT NULL AND old_j IS NOT NULL AND (new_j -> win) IS DISTINCT FROM (old_j -> win) THEN
            d := new_v;
        ELSIF zero_reset AND old_j IS NOT NULL AND new_v = 0 AND old_v <> 0 THEN
            resets := resets || to_jsonb(col);
            CONTINUE;
        ELSE
            d := new_v - old_v;
        END IF;
        IF d <> 0 THEN
            deltas := deltas || jsonb_build_object(col, d::TEXT);
        END IF;
    END LOOP;

    IF deltas = '{}'::jsonb AND jsonb_array_length(resets) = 0 THEN
        RETURN NULL;
    END IF;
    FOREACH c IN ARRAY pk_cols LOOP
        pk_j := pk_j || jsonb_build_object(c, new_j -> c);
    END LOOP;
    reason := COALESCE(NULLIF(current_setting('sub2api.region_sync_reason', true), ''), 'balance_change');
    stream_no := CASE WHEN reason = 'usage' THEN 3 ELSE 2 END;
    INSERT INTO region_sync_outbox (origin_region, stream, entity_type, entity_key, payload)
    VALUES (cfg.region_id, stream_no, TG_TABLE_NAME, region_sync_entity_key(new_j, pk_cols),
            jsonb_build_object('op', 'delta', 'pk', pk_j, 'd', deltas, 'z', resets,
                               'reason', reason,
                               'ref', NULLIF(current_setting('sub2api.region_sync_ref', true), '')));
    IF stream_no = 2 THEN
        PERFORM pg_notify('region_sync_wake', '2');
    END IF;
    RETURN NULL;
END;
$$;

-- 非主地区把本地使用记录发往主地区汇总；主地区导入时保留来源 id 映射。
CREATE OR REPLACE FUNCTION region_sync_capture_usage_log()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    cfg region_sync_local%ROWTYPE;
BEGIN
    IF region_sync_applying() THEN
        RETURN NULL;
    END IF;
    SELECT * INTO cfg FROM region_sync_local WHERE id = 1;
    IF NOT FOUND OR NOT cfg.enabled OR NOT cfg.ship_usage THEN
        RETURN NULL;
    END IF;
    INSERT INTO region_sync_outbox (origin_region, stream, entity_type, entity_key, payload)
    VALUES (cfg.region_id, 3, 'usage_logs', NEW.id::TEXT,
            jsonb_build_object('op', 'usage_log', 'row', to_jsonb(NEW)));
    RETURN NULL;
END;
$$;

-- 以下触发器由 regionsync.TriggerDDL() 生成，集成测试校验与注册表一致。

DROP TRIGGER IF EXISTS trg_region_sync_cfg_users ON users;
CREATE TRIGGER trg_region_sync_cfg_users
AFTER INSERT OR UPDATE OR DELETE ON users
FOR EACH ROW EXECUTE FUNCTION region_sync_capture_config(
    'id',
    'del=primary',
    'admin:primary:role,concurrency,status,notes,rpm_limit,restrict_public_groups,signup_source,wechat,deleted_at',
    'profile:any:username,balance_notify_enabled,balance_notify_threshold,balance_notify_threshold_type,balance_notify_extra_emails',
    'cred:any:email,password_hash,totp_secret_encrypted,totp_enabled,totp_enabled_at',
    'meta:insert:created_at');

DROP TRIGGER IF EXISTS trg_region_sync_cfg_api_keys ON api_keys;
CREATE TRIGGER trg_region_sync_cfg_api_keys
AFTER INSERT OR UPDATE OR DELETE ON api_keys
FOR EACH ROW EXECUTE FUNCTION region_sync_capture_config(
    'id',
    'del=any',
    'ident:any:user_id,key',
    'cfg:any:name,group_id,status,ip_whitelist,ip_blacklist,quota,expires_at,rate_limit_5h,rate_limit_1d,rate_limit_7d',
    'life:any:deleted_at',
    'meta:insert:created_at');

DROP TRIGGER IF EXISTS trg_region_sync_cfg_groups ON groups;
CREATE TRIGGER trg_region_sync_cfg_groups
AFTER INSERT OR UPDATE OR DELETE ON groups
FOR EACH ROW EXECUTE FUNCTION region_sync_capture_config(
    'id',
    'del=primary',
    'cfg:primary:name,description,rate_multiplier,is_exclusive,status,deleted_at,platform,subscription_type,daily_limit_usd,weekly_limit_usd,monthly_limit_usd,default_validity_days,image_price_1k,image_price_2k,image_price_4k,claude_code_only,fallback_group_id,model_routing,model_routing_enabled,fallback_group_id_on_invalid_request,mcp_xml_inject,supported_model_scopes,sort_order,allow_messages_dispatch,default_mapped_model,require_oauth_only,require_privacy_set,messages_dispatch_model_config,rpm_limit,allow_image_generation,image_rate_independent,image_rate_multiplier,model_allowlist,peak_rate_enabled,peak_start,peak_end,peak_rate_multiplier,batch_image_discount_multiplier,batch_image_hold_multiplier,allow_batch_image_generation,video_rate_independent,video_rate_multiplier,video_price_480p,video_price_720p,video_price_1080p,web_search_price_per_call,duplicate_operation_id,max_reasoning_effort,reasoning_effort_mappings,allow_live,profit_control_enabled,profit_min_margin,profit_safety_buffer,video_model_prices,audio_realtime_price_per_min,audio_tts_price_per_million_chars,audio_stt_price_per_hour,search_price_per_1k,long_context_pricing_enabled,model_pricing,force_openai_fast,max_reasoning_effort_over_limit,free_openai_fast,codex_models_manifest_config,i18n',
    'meta:insert:created_at');

DROP TRIGGER IF EXISTS trg_region_sync_cfg_user_allowed_groups ON user_allowed_groups;
CREATE TRIGGER trg_region_sync_cfg_user_allowed_groups
AFTER INSERT OR UPDATE OR DELETE ON user_allowed_groups
FOR EACH ROW EXECUTE FUNCTION region_sync_capture_config(
    'user_id,group_id',
    'del=primary',
    'meta:primary:created_at');

DROP TRIGGER IF EXISTS trg_region_sync_cfg_user_group_rate_multipliers ON user_group_rate_multipliers;
CREATE TRIGGER trg_region_sync_cfg_user_group_rate_multipliers
AFTER INSERT OR UPDATE OR DELETE ON user_group_rate_multipliers
FOR EACH ROW EXECUTE FUNCTION region_sync_capture_config(
    'user_id,group_id',
    'del=primary',
    'cfg:primary:rate_multiplier,rpm_override',
    'meta:insert:created_at');

DROP TRIGGER IF EXISTS trg_region_sync_cfg_user_subscriptions ON user_subscriptions;
CREATE TRIGGER trg_region_sync_cfg_user_subscriptions
AFTER INSERT OR UPDATE OR DELETE ON user_subscriptions
FOR EACH ROW EXECUTE FUNCTION region_sync_capture_config(
    'id',
    'del=primary',
    'cfg:primary:user_id,group_id,starts_at,expires_at,status,assigned_by,assigned_at,notes,deleted_at',
    'meta:insert:created_at,daily_window_start,weekly_window_start,monthly_window_start');

DROP TRIGGER IF EXISTS trg_region_sync_cfg_user_platform_quotas ON user_platform_quotas;
CREATE TRIGGER trg_region_sync_cfg_user_platform_quotas
AFTER INSERT OR UPDATE OR DELETE ON user_platform_quotas
FOR EACH ROW EXECUTE FUNCTION region_sync_capture_config(
    'id',
    'del=primary',
    'cfg:primary:user_id,platform,daily_limit_usd,weekly_limit_usd,monthly_limit_usd,deleted_at',
    'meta:insert:created_at');

DROP TRIGGER IF EXISTS trg_region_sync_cfg_accounts ON accounts;
CREATE TRIGGER trg_region_sync_cfg_accounts
AFTER INSERT OR UPDATE OR DELETE ON accounts
FOR EACH ROW EXECUTE FUNCTION region_sync_capture_config(
    'id',
    'del=primary',
    'cfg:primary:name,notes,platform,type,proxy_id,proxy_fallback_origin_id,concurrency,load_factor,priority,rate_multiplier,status,schedulable,expires_at,auto_pause_on_expired,parent_account_id,quota_dimension,deleted_at',
    'extra:primary:extra*quota_used|quota_daily_used|quota_daily_start|quota_daily_reset_at|quota_weekly_used|quota_weekly_start|quota_weekly_reset_at|model_rate_limits',
    'cred:any:credentials',
    'meta:insert:created_at');

DROP TRIGGER IF EXISTS trg_region_sync_cfg_account_groups ON account_groups;
CREATE TRIGGER trg_region_sync_cfg_account_groups
AFTER INSERT OR UPDATE OR DELETE ON account_groups
FOR EACH ROW EXECUTE FUNCTION region_sync_capture_config(
    'account_id,group_id',
    'del=primary',
    'cfg:primary:priority',
    'meta:insert:created_at');

DROP TRIGGER IF EXISTS trg_region_sync_cfg_proxies ON proxies;
CREATE TRIGGER trg_region_sync_cfg_proxies
AFTER INSERT OR UPDATE OR DELETE ON proxies
FOR EACH ROW EXECUTE FUNCTION region_sync_capture_config(
    'id',
    'del=primary',
    'cfg:primary:name,protocol,host,port,username,password,status,expires_at,fallback_mode,backup_proxy_id,expiry_warn_days,deleted_at',
    'meta:insert:created_at');

DROP TRIGGER IF EXISTS trg_region_sync_cfg_channels ON channels;
CREATE TRIGGER trg_region_sync_cfg_channels
AFTER INSERT OR UPDATE OR DELETE ON channels
FOR EACH ROW EXECUTE FUNCTION region_sync_capture_config(
    'id',
    'del=primary',
    'cfg:primary:name,description,status,model_mapping,billing_model_source,restrict_models,features,apply_pricing_to_account_stats,features_config',
    'meta:insert:created_at');

DROP TRIGGER IF EXISTS trg_region_sync_cfg_channel_groups ON channel_groups;
CREATE TRIGGER trg_region_sync_cfg_channel_groups
AFTER INSERT OR UPDATE OR DELETE ON channel_groups
FOR EACH ROW EXECUTE FUNCTION region_sync_capture_config(
    'id',
    'del=primary',
    'cfg:primary:channel_id,group_id',
    'meta:insert:created_at');

DROP TRIGGER IF EXISTS trg_region_sync_cfg_channel_model_pricing ON channel_model_pricing;
CREATE TRIGGER trg_region_sync_cfg_channel_model_pricing
AFTER INSERT OR UPDATE OR DELETE ON channel_model_pricing
FOR EACH ROW EXECUTE FUNCTION region_sync_capture_config(
    'id',
    'del=primary',
    'cfg:primary:channel_id,models,input_price,output_price,cache_write_price,cache_read_price,image_output_price,billing_mode,per_request_price,platform,image_input_price,time_pricing,fast_multiplier,flex_multiplier,cache_write_1h_price,max_reasoning_effort_multiplier,reasoning_effort_multipliers',
    'meta:insert:created_at');

DROP TRIGGER IF EXISTS trg_region_sync_cfg_channel_pricing_intervals ON channel_pricing_intervals;
CREATE TRIGGER trg_region_sync_cfg_channel_pricing_intervals
AFTER INSERT OR UPDATE OR DELETE ON channel_pricing_intervals
FOR EACH ROW EXECUTE FUNCTION region_sync_capture_config(
    'id',
    'del=primary',
    'cfg:primary:pricing_id,min_tokens,max_tokens,tier_label,input_price,output_price,cache_write_price,cache_read_price,per_request_price,sort_order,input_multiplier,output_multiplier,cache_write_multiplier,cache_read_multiplier,cache_write_1h_price',
    'meta:insert:created_at');

DROP TRIGGER IF EXISTS trg_region_sync_cfg_composite_model_routes ON composite_model_routes;
CREATE TRIGGER trg_region_sync_cfg_composite_model_routes
AFTER INSERT OR UPDATE OR DELETE ON composite_model_routes
FOR EACH ROW EXECUTE FUNCTION region_sync_capture_config(
    'id',
    'del=primary',
    'cfg:primary:group_id,public_model,match_type,target_platform,upstream_model,endpoint,priority,enabled,notes,deleted_at',
    'meta:insert:created_at');

DROP TRIGGER IF EXISTS trg_region_sync_cfg_error_passthrough_rules ON error_passthrough_rules;
CREATE TRIGGER trg_region_sync_cfg_error_passthrough_rules
AFTER INSERT OR UPDATE OR DELETE ON error_passthrough_rules
FOR EACH ROW EXECUTE FUNCTION region_sync_capture_config(
    'id',
    'del=primary',
    'cfg:primary:name,enabled,priority,error_codes,keywords,match_mode,platforms,passthrough_code,response_code,passthrough_body,custom_message,description,skip_monitoring',
    'meta:insert:created_at');

DROP TRIGGER IF EXISTS trg_region_sync_cfg_tls_fingerprint_profiles ON tls_fingerprint_profiles;
CREATE TRIGGER trg_region_sync_cfg_tls_fingerprint_profiles
AFTER INSERT OR UPDATE OR DELETE ON tls_fingerprint_profiles
FOR EACH ROW EXECUTE FUNCTION region_sync_capture_config(
    'id',
    'del=primary',
    'cfg:primary:name,description,enable_grease,cipher_suites,curves,point_formats,signature_algorithms,alpn_protocols,supported_versions,key_share_groups,psk_modes,extensions',
    'meta:insert:created_at');

DROP TRIGGER IF EXISTS trg_region_sync_cfg_settings ON settings;
CREATE TRIGGER trg_region_sync_cfg_settings
AFTER INSERT OR UPDATE OR DELETE ON settings
FOR EACH ROW EXECUTE FUNCTION region_sync_capture_config(
    'key',
    'del=primary',
    'cfg:primary:value');

DROP TRIGGER IF EXISTS trg_region_sync_cfg_passkey_credentials ON passkey_credentials;
CREATE TRIGGER trg_region_sync_cfg_passkey_credentials
AFTER INSERT OR UPDATE OR DELETE ON passkey_credentials
FOR EACH ROW EXECUTE FUNCTION region_sync_capture_config(
    'id',
    'del=any',
    'cfg:any:user_id,credential_id,name,credential_data',
    'meta:insert:created_at');

DROP TRIGGER IF EXISTS trg_region_sync_cfg_passkey_user_handles ON passkey_user_handles;
CREATE TRIGGER trg_region_sync_cfg_passkey_user_handles
AFTER INSERT OR UPDATE OR DELETE ON passkey_user_handles
FOR EACH ROW EXECUTE FUNCTION region_sync_capture_config(
    'user_id',
    'del=any',
    'cfg:any:user_handle',
    'meta:insert:created_at');

DROP TRIGGER IF EXISTS trg_region_sync_delta_users ON users;
CREATE TRIGGER trg_region_sync_delta_users
AFTER INSERT OR UPDATE ON users
FOR EACH ROW EXECUTE FUNCTION region_sync_capture_delta(
    'id',
    'balance',
    'frozen_balance',
    'total_recharged');

DROP TRIGGER IF EXISTS trg_region_sync_delta_api_keys ON api_keys;
CREATE TRIGGER trg_region_sync_delta_api_keys
AFTER INSERT OR UPDATE ON api_keys
FOR EACH ROW EXECUTE FUNCTION region_sync_capture_delta(
    'id',
    'quota_used!');

DROP TRIGGER IF EXISTS trg_region_sync_delta_user_subscriptions ON user_subscriptions;
CREATE TRIGGER trg_region_sync_delta_user_subscriptions
AFTER INSERT OR UPDATE ON user_subscriptions
FOR EACH ROW EXECUTE FUNCTION region_sync_capture_delta(
    'id',
    'daily_usage_usd@daily_window_start',
    'weekly_usage_usd@weekly_window_start',
    'monthly_usage_usd@monthly_window_start');

DROP TRIGGER IF EXISTS trg_region_sync_usage_logs ON usage_logs;
CREATE TRIGGER trg_region_sync_usage_logs
AFTER INSERT ON usage_logs
FOR EACH ROW EXECUTE FUNCTION region_sync_capture_usage_log();
