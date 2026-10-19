-- MY/GZ 多地区同步回滚。两地都要执行。
--
-- 第 1 级（推荐，可随时再开）：config.yaml 里 region_sync.enabled=false 重启即可，
-- 服务启动时会把 region_sync_local.enabled 置 false，触发器全部变成空操作。
-- 换回不认识该功能的旧二进制时，旧二进制不会去关开关，必须先手动执行：
--
--   UPDATE region_sync_local SET enabled = FALSE, ship_usage = FALSE;
--
-- 第 2 级（彻底移除）：先确认两地积压都为 0（sub2api region-sync status），再执行下面整段。

BEGIN;

UPDATE region_sync_local SET enabled = FALSE, ship_usage = FALSE;

DO $$
DECLARE r RECORD;
BEGIN
    FOR r IN SELECT tgname, tgrelid::regclass AS tbl FROM pg_trigger WHERE tgname LIKE 'trg_region_sync_%' LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', r.tgname, r.tbl);
    END LOOP;
END $$;

DROP FUNCTION IF EXISTS region_sync_capture_usage_log();
DROP FUNCTION IF EXISTS region_sync_capture_delta();
DROP FUNCTION IF EXISTS region_sync_capture_config();
DROP FUNCTION IF EXISTS region_sync_bump_version(TEXT, TEXT, TEXT, TEXT);
DROP FUNCTION IF EXISTS region_sync_entity_key(JSONB, TEXT[]);
DROP FUNCTION IF EXISTS region_sync_project(JSONB, TEXT[]);
DROP FUNCTION IF EXISTS region_sync_applying();

DROP TABLE IF EXISTS region_sync_baseline, region_sync_usage_import, region_sync_pending_invalidations,
    region_sync_deferred, region_sync_versions, region_sync_inbox, region_sync_outbox, region_sync_local;

-- 主键序列恢复步长 1（当前值保留，只是之后连续分配）。
ALTER SEQUENCE IF EXISTS users_id_seq INCREMENT BY 1;
ALTER SEQUENCE IF EXISTS api_keys_id_seq INCREMENT BY 1;
ALTER SEQUENCE IF EXISTS passkey_credentials_id_seq INCREMENT BY 1;

-- 让以后重新升级时迁移 246 能再次执行。
DELETE FROM schema_migrations WHERE filename = '246_region_sync_foundation.sql';

COMMIT;
