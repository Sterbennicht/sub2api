# MY / GZ 异地多活

默认关闭（`region_sync.enabled: false`），关闭时与单节点完全一致：触发器是空操作、不挂守卫、不注册内部接口。

## 拓扑

| | GZ（广州） | MY（马来西亚） |
|---|---|---|
| 角色 | 主节点：配置主写、管理后台（`primary_region: gz`） | 从节点；后台 GET 读本地，后台写转发到 GZ |
| 模型请求 / 鉴权 / 扣费 | 本地库 + 本地 Redis | 本地库 + 本地 Redis |
| 用户登录、个人设置、自助 Key | 本地 | 本地，异步同步，不依赖 GZ 在线 |
| 注册、兑换、下单、第三方登录、支付回调 | 本地 | 转发到 GZ（GZ 不可达时返回 502 `REGION_PRIMARY_UNAVAILABLE`） |
| 使用记录 | 汇总地区（`usage_aggregation_region: gz`），全局用量与账务总览在这里 | 单向发往 GZ（来源 id 映射防重） |

选 GZ 当主节点：不用搬主库、回滚简单；MY 只有 842MB 内存且是突发型实例；支付回调和邮件推送离 GZ 近。
换成 MY 为主只改配置：两边 `primary_region: my`，GZ 设 `forward_primary_writes: true`。

### 管理后台入口（sub2api.example.com）

两种挂法，域名都不暴露大陆 IP：

- **默认：MY 本地读 + 写转发**。`/api/v1/admin` 的 GET 由 MY 本地库返回（可能落后几秒），写由 MY 的 Sub2API 转发到 GZ。
  断链时后台只读可用、写失败。
- **整体反代**：MY 的 nginx 把 `/api/v1/admin`（以及后台页面所需的接口）直接 `proxy_pass` 到 GZ，后台数据永远是 GZ 的实时数据。
  断链时 MY 侧后台整体不可用。
  ```nginx
  location /api/v1/admin/ {
      proxy_pass https://192.0.2.10:8443;
      proxy_ssl_verify off;
      proxy_set_header Host $host;
      proxy_set_header X-Real-IP $remote_addr;
  }
  ```
  后台前端页面是同一个 SPA，由 MY 本地返回即可，只有接口走 GZ。

两种挂法下，断链都不影响用户中心（登录、个人设置、自助 Key、passkey）和模型请求。

请求路径上没有任何跨区调用。跨区只有后台 worker：
`POST /internal/region-sync/v1/events`、`/reconcile`，以及 `GET /status`（同样要签名）。
认证为 HMAC-SHA256，签名覆盖来源、时间戳、方法、路径和 body 摘要，允许 5 分钟时钟偏差。

## 数据规则

- **配置**：触发器在业务事务内写 `region_sync_outbox`，按字段组带 HLC 版本（微秒、地区）。
  接收端按组比较版本，旧的不覆盖新的。硬删除留墓碑，Key 软删除是粘性的，不会被复活。
- **余额类**（balance、frozen_balance、total_recharged、Key 已用额度、订阅窗口用量）：发精确十进制增量（text），
  接收端执行 `col = col + delta`，从不整行覆盖。充值、退款、调账、冻结、结算、释放走高优先级流 2，消费走批量流 3。
- **只在本地**：并发槽、排队、冷却、熔断、粘性会话（Redis）、账号运行时计数（`accounts.extra` 的 quota_*、model_rate_limits）、
  账号 `error` 状态（同步只接受 active/disabled）、refresh token、step-up 授权、平台额度用量计数。
- **OAuth**：`extra.region_pool` 限定可调度地区；刷新 owner 依次取 `extra.region_refresh_owner`、单地区池、`default_oauth_refresh_owner`。
  非 owner 不刷新，只使用同步过来的凭据。

### 字段修改权限与冲突规则

| 表 | 字段组 | 谁能改并下发 | 冲突 |
|---|---|---|---|
| users | role、concurrency、status、notes、rpm_limit、限制分组等（admin） | 只认主地区；非主地区的本地改动不外发，下次主地区下发时被覆盖 | 主地区版本 |
| users | username、通知邮箱（profile） | 任一地区（用户自己） | 最后写入者胜 |
| users | email、password_hash、totp_*（cred） | 任一地区 | 最后写入者胜 |
| api_keys | 名称、分组、状态、IP 名单、额度、过期、限速 | 任一地区（用户自助 / 管理员） | 最后写入者胜；删除粘性 |
| api_keys | quota_used | 增量；清零以 reset 下发 | 累加 |
| accounts | 配置、extra（不含运行时 key） | 只认主地区 | 主地区版本 |
| accounts | credentials | 任一地区（refresh owner 刷新） | 最后写入者胜 |
| groups、channels、proxies、settings 等 | 全部 | 只认主地区 | 主地区版本 |

各组之间互不影响：MY 用户改用户名，不会被 GZ 管理员同一时刻改的备注覆盖；反过来也一样。完整清单见 `backend/internal/regionsync/tables.go`。
启动时如果发现有列没分类，会打 `region_sync_unclassified_columns` 告警。

## 发送参数（默认）

配置变更：提交即唤醒（LISTEN/NOTIFY），另有 1s 扫描兜底。账务：1s。消费：2s 一批，每批最多 500 条或 512KiB，有积压时连续补发。
失败退避 1/2/4/8/16/30s，带抖动。恢复后先补配置和账务，再补消费。每 5 分钟对账一次（48h 窗口，按 1000 条分段比对 md5，缺的重新入队）。
接收端按 event_id 去重，每批一个事务、每条事件一个 savepoint，提交后才确认，所以重发和丢确认都不会重复扣费。
上游模型调用本身不是 exactly-once，同步也不声称做到。

## 初始化

1. 两地部署同一版本，迁移 246 自动执行。MY 加 swap（已加 3G），并给 PG 与 Sub2API 设内存上限。
2. **GZ**：按 `config.gz.yaml` 配置，先设 `sender_enabled: false` 启动，开始捕获但不发送。
3. **GZ**：`sub2api region-sync partition-sequences --offset 2 --execute`
4. **GZ**：`sub2api region-sync baseline-snapshot`，保持运行。在另一个终端执行它打印的 `pg_dump --snapshot=...` 命令
   （命令里已排除同步队列表的数据），dump 完成后回车。
   快照内已有的待发事件会被标成 `baseline`，快照之后提交的事件照常发送。
5. **MY**：建空库，执行 `pg_restore --no-owner -d sub2api region-baseline.dump`，然后：
   ```
   sub2api region-sync baseline-import --source gz --execute
   sub2api region-sync partition-sequences --offset 1 --execute
   ```
6. **MY**：按 `config.my.yaml` 配置，两地注入相同的 `REGION_SYNC_PEER_SHARED_SECRET`，`jwt.secret` 与 GZ 相同，然后启动。
7. **GZ**：改 `sender_enabled: true` 后重启。用 `region-sync status` 看两地积压降到 0，再用 `region-sync checksum` 比对两地是否一致。
8. 切 nginx：`sub2api.example.com` 和 MY 的 8443 改为指向 MY 本机 Sub2API（现在是中继到 GZ）。
   GZ nginx 要加 `set_real_ip_from 192.0.2.20;`，让 MY 转发的写请求带上真实客户端 IP。

## 监控

- 在服务器上运行 `sudo -u sub2api /opt/sub2api/sub2api region-sync status`（在 /opt/sub2api 目录下）：查看各流积压、最老事件年龄、暂存、拒绝、待失效缓存。
- 日志关键词：`region_sync_backlog_alert`（积压超过 300s）、`region_sync_send_failed`、`region_sync_event_rejected*`、`region_sync_attention`。
- 被拒绝的事件仍会确认，原始负载存进 `region_sync_deferred(kind='rejected')`。修好原因后执行 `sub2api region-sync replay-rejected --execute`。

## 长时间断链

两地各自照常服务，事件持久积压在 outbox（7 天保留期只清理已确认的）。积压超过 300s 时告警。
恢复后先补配置和账务，再补消费，补消费期间按 `catchup_max_batches_per_second`（默认 10）节流。
测试里 1500 次扣费加 1500 条使用记录的积压，补发期间本地扣费 p95 从 9.2ms 变成 10.7ms。
那次跑在 Docker Desktop 上（往返约 0.4ms），每条使用记录大约 6ms，主要耗在往返上。生产环境用本机 socket，会快一个数量级。
断链期间两地可以各自消费同一份余额，会出现短时超额，事件交换后收敛。全局限速也会变宽。

## 回滚

- 软回滚：`region_sync.enabled: false` 后重启，触发器立即变成空操作，单节点行为恢复。
- 换回旧二进制前，先执行 `UPDATE region_sync_local SET enabled = FALSE, ship_usage = FALSE;`，否则旧二进制下 outbox 会一直增长。
- 彻底移除：先确认两地积压为 0，再执行 `rollback.sql`。它会删除触发器、函数和表，把序列步长恢复为 1，并删掉迁移 246 的记录。
- MY 回到「中继到 GZ」：关掉 MY 侧 Sub2API，入口（nginx）切回中继 GZ 即可，不用反向搬数据。GZ 是主库，数据始终完整；MY 断开前未发出的本地变更（多为用户自助改动和本地扣费）以 GZ 已收到的为准，切换前先看 `region-sync status` 让 MY 积压降到 0。

## 残留依赖与已知缺口

- **出口**：账号代理 `127.0.0.1:789x` 只在 GZ 有。MY 需要本地 mihomo，或用 `proxy_url_overrides` 改指向本地代理；写 `direct` 表示该代理在 MY 直连。
  aihub.top、anyrouter、ai2api 从 GZ 直连不通（现在已经走代理）。各地区的出口要分别验证。
- **MY → GZ 依赖**：只有管理写、注册、兑换、支付、第三方登录（转发）。GZ 挂了时 MY 的这些操作失败，其余照常。GZ 不依赖 MY（MY 断开时 GZ 只是暂时收不到 MY 的使用记录，恢复后补齐）。
- **step-up**：授权存在各地区 Redis。如果开了 `step_up_enabled`，MY 转发到 GZ 的受保护写会失败（线上目前没开）。
  插件、备份、数据管理、运维等写操作留在 MY 本地执行（`local_admin_write_prefixes`）。
- **不同步的表**：user_avatars、auth_identities（登录时在本地回填）。
- settings 进程内缓存按 TTL 过期，不主动失效。
- 订阅窗口与 Key 额度重置按近似处理：窗口重置以新值下发，清零以 reset 下发，与跨区并发增量交错时可能有小误差。
- 断链期间一地的 Redis 余额缓存只在收到对端增量后失效，所以短时超额无法避免（这是允许的）。
