package regionsync

import (
	"fmt"
	"strings"
)

func sqlLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// TriggerDDL 生成迁移里的触发器定义；迁移文件中的触发器段就是它的输出。
func TriggerDDL() string {
	var b strings.Builder
	for _, t := range ConfigTables {
		args := t.TriggerArgs()
		quoted := make([]string, len(args))
		for i, a := range args {
			quoted[i] = sqlLiteral(a)
		}
		name := "trg_region_sync_cfg_" + t.Name
		fmt.Fprintf(&b, "DROP TRIGGER IF EXISTS %s ON %s;\n", name, t.Name)
		fmt.Fprintf(&b, "CREATE TRIGGER %s\nAFTER INSERT OR UPDATE OR DELETE ON %s\nFOR EACH ROW EXECUTE FUNCTION region_sync_capture_config(\n    %s);\n\n",
			name, t.Name, strings.Join(quoted, ",\n    "))
	}
	for _, t := range DeltaTables {
		args := t.TriggerArgs()
		quoted := make([]string, len(args))
		for i, a := range args {
			quoted[i] = sqlLiteral(a)
		}
		name := "trg_region_sync_delta_" + t.Name
		fmt.Fprintf(&b, "DROP TRIGGER IF EXISTS %s ON %s;\n", name, t.Name)
		fmt.Fprintf(&b, "CREATE TRIGGER %s\nAFTER INSERT OR UPDATE ON %s\nFOR EACH ROW EXECUTE FUNCTION region_sync_capture_delta(\n    %s);\n\n",
			name, t.Name, strings.Join(quoted, ",\n    "))
	}
	_, _ = b.WriteString("DROP TRIGGER IF EXISTS trg_region_sync_usage_logs ON usage_logs;\n")
	_, _ = b.WriteString("CREATE TRIGGER trg_region_sync_usage_logs\nAFTER INSERT ON usage_logs\nFOR EACH ROW EXECUTE FUNCTION region_sync_capture_usage_log();\n")
	return b.String()
}
