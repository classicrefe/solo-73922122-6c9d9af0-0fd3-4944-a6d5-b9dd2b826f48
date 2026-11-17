# Traffic PITR Recovery API（交通数仓误删恢复 + 乱序补传）

本服务提供两部分能力：

1. **有序批量采集**：路口采集端断网后会乱序补传分钟车流，服务按路口序号去重、
   缺口补齐后按序写入现有分钟计数表，并向值班员提供已发布水位与下一缺号。
2. **PITR 误删恢复**：路口分钟计数被值班员误删后，从**物理备份 + WAL 归档**把删除前
   的记录找回来补数，恢复期间主库采集不中断：启动时对主库做一次 `pg_basebackup`
   物理基准备份，主库持续归档 WAL；用户可创建**命名恢复点**，只有该点所需 WAL 全部
   归档后才允许选择它恢复；每次恢复都从基准备份**重建一个全新的隔离只读副本**，
   重放 WAL 到目标恢复点即停，供查询补数。

## 乱序批量采集

### 数据模型

源库新增两张采集台账表（`db/init/01-init.sql`；API 启动时也会幂等补建，旧数据卷可原地升级）：

| 表 | 含义 |
|---|---|
| `traffic_ingest_observations` | 每条被接受的观测，主键 `(intersection_id, seq)` 即去重键；含 `minute_utc`、`vehicle_count`、`received_at`。 |
| `traffic_ingest_watermarks` | 每个路口已**按序发布**到 `traffic_minute_counts` 的最大连续序号 `published_seq`；`published_seq + 1` 即下一缺号。 |
| `traffic_minute_counts` | 原有分钟计数表，仍是对外查询的结果表。 |

### 语义

- **序号从 1 开始**；每个请求是一个采集端的补传批次，记录可任意乱序。
- **缺号记录持久待发布**：高于缺口的观测立即落库（`pending`），水位不动；服务重启、
  连接断开后仍可查询（台账在主库，不在进程内存）。
- **缺口补齐按序发布**：每次提交后从水位 +1 起取连续前缀，按 `seq` 升序写入
  `traffic_minute_counts`；同一分钟出现多条时，**高序号修订覆盖较早值**。
- **幂等重试**：同一 `(路口, 序号)` 同内容重复提交不重复写入、不改变结果；同一批次内
  完全重复的记录也被折叠。
- **异内容整批拒绝**：任一路口同一序号内容不同（含同批次内重复序号内容不同），整批
  返回 409，**观测、主表、水位全部回滚**，响应列出全部冲突明细。
- **同成同败**：观测写入、主表 upsert、水位推进在同一个 PostgreSQL 事务中提交或回滚。
- **水位单调**：`published_seq` 只增不减（旧批次重放不会让水位回退）。
- **按路口隔离**：事务内对每个路口取 `pg_advisory_xact_lock`（按路口名排序取锁，
  多路口批次不会死锁）。同一路口串行，其他路口互不阻塞；等待超过
  `INGEST_LOCK_TIMEOUT_MS` 返回 503 且不写入，采集端可安全重试。
- **校验**：`seq >= 1`、`vehicle_count >= 0`、`minute_utc` 必须是整分钟（无偏移的时间
  按 UTC 解释），违反返回 422。

### 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/ingest/batches` | 提交补传批次 `{"records":[{intersection_id, seq, minute_utc, vehicle_count}]}`；返回每条记录 `published/pending`、本次新发布序号、下一缺号、待发布序号。409=异内容整批拒绝；422=校验失败；503=同路口并发锁超时/存储失败（均未写入）。 |
| GET | `/ingest/watermarks` | 全部路口的水位、下一缺号、待发布条数与首/末待发布序号。 |
| GET | `/ingest/watermarks/{intersection_id}` | 单路口水位、下一缺号、待发布序号（未见过该路口返回 404）。 |
| GET | `/ingest/pending/{intersection_id}` | 单路口已持久化但尚未发布的观测（分页）。 |
| GET | `/ingest/observations/{intersection_id}?status=all\|pending\|published` | 单路口台账观测及发布状态（分页）。 |
| GET | `/ingest/traffic/{intersection_id}` | 单路口在主表中的当前结果行。 |

原有的恢复/演示接口（`/demo/*`、`/restore-points`、`/recoveries`、`/replica/traffic`、
`/primary/traffic`、`/backup`、`/health`）全部保留，语义未变。

### 演示

```bash
docker compose up --build
# Swagger UI: http://localhost:8000/docs
./scripts/ingest-demo.sh   # 乱序补传 → 待发布 → 缺口补齐 → 修订覆盖 → 幂等/冲突
./scripts/demo.sh          # 原有 PITR 误删恢复演示
```

`ingest-demo.sh`（curl+jq）完整走一遍：seq 1/3/4 乱序到达（3、4 待发布）→ 水位与
`next_missing_seq=2` → 补 seq 2 后 2..4 按序发布 → seq 5 修订同一分钟（99 覆盖 21）
→ 原样重传不重复写 → 用不同内容重传 seq 3 返回 409 且水位不变。

## 架构

```
┌──────────────┐   WAL 归档(archive_command)   ┌───────────────┐
│  db (主库)    │ ───────────────────────────▶ │  wal_archive  │
│  traffic 库   │                              │  (共享卷)      │
│  业务表:      │ ◀──────── pg_basebackup ───┐ │               │
│  traffic_     │                            │ │               │
│  minute_      │        ┌───────────────────┴─┴─────────────┐ │
│  counts       │        │  api (FastAPI)                    │ │
│  + 采集台账   │        │  · 乱序补传批次/水位/待发布查询     │ │
└──────────────┘        │  · 启动时物理基准备份 (/backups)    │ │
       ▲                │  · 恢复点就绪检查 (WAL 连续性)       │ │
       │ 主库采集不中断   │  · 恢复执行器: 每次从基准备份重建    │ │
       └────────────────│    隔离副本, 重放 WAL 到恢复点暂停   │ │
                        │    (/recovery_work/rN, 只读热备)     │ │
                        └─────────────────────────────────────┘ │
```

- **db**：`postgres:16`，开启 `archive_mode=on`、`archive_timeout=10s`，WAL 归档到共享卷。
  业务表为 `traffic_minute_counts` 加上文两张采集台账表（同属业务数据，随基准备份/WAL
  一起被恢复）。
- **api**：`postgres:16` 镜像 + FastAPI。启动时执行 `pg_basebackup -X stream` 建立物理
  基准备份；恢复时把基准备份复制到全新目录，配置 `restore_command` +
  `recovery_target_name` + `recovery_target_action=pause`，以热备只读方式启动在
  独立端口（容器内 127.0.0.1:55433）。
- API 自身的运行台账（恢复点、恢复任务）存在独立 SQLite（api_state 卷），不进入恢复
  目标；采集台账属于业务数据，存于主库。

## 快速开始

```bash
docker compose up --build
# 等待 api 就绪后:
# 1) 打开演示入口 http://localhost:8000/docs （Swagger UI，根路径 / 会重定向到这里）
# 2) 或跑一键演示脚本（需要 curl 和 jq）:
./scripts/ingest-demo.sh   # 乱序批量采集
./scripts/demo.sh          # PITR 误删恢复
```

`demo.sh` 完整走一遍：模拟采集写入 → 创建恢复点 `before_delete` → 等待 WAL 归档齐全 →
模拟误删最近 10 分钟 → 主库继续写入新路口数据 → 发起 PITR 恢复 → 从只读副本查回被删
记录 → 断言副本不含恢复点之后的提交、主库数据未受影响。

## API 一览（详见 /docs）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 服务与主库连通性 |
| GET | `/backup` | 启动基准备份状态（running/ready/failed + 错误） |
| POST | `/ingest/batches` | 提交乱序补传批次（见上节「乱序批量采集」） |
| GET | `/ingest/watermarks` / `/ingest/watermarks/{id}` | 路口已发布水位与下一缺号 |
| GET | `/ingest/pending/{id}` | 待发布观测（分页） |
| GET | `/ingest/observations/{id}` | 观测台账及状态（分页，可按状态过滤） |
| GET | `/ingest/traffic/{id}` | 某路口在主表中的当前结果 |
| POST | `/demo/generate` | 模拟采集：生成 路口×分钟 计数（可指定前缀/数量） |
| POST | `/demo/delete` | 模拟误删：删除最近 N 分钟数据 |
| GET | `/primary/traffic` | 查主库当前数据（证明主库一直在采集） |
| GET | `/demo/overview` | 主库数据、备份、恢复点、恢复任务总览 |
| POST | `/restore-points?name=...` | 创建命名恢复点（`pg_create_restore_point` + 强制切 WAL） |
| GET | `/restore-points` | 恢复点列表：`pending`（WAL 未齐）/ `ready`（可选）/ `expired`（早于基准备份） |
| POST | `/recoveries` | 发起恢复 `{"restore_point": "name"}`，202 异步任务；同一时刻只允许一项在途 |
| GET | `/recoveries` / `/recoveries/{id}` | 恢复任务状态：running/success/failed/superseded + 错误详情 |
| GET | `/recoveries/current` | 当前对外服务的恢复（无则 404） |
| GET | `/replica/traffic` | 查询已恢复只读副本（未就绪/已失败返回 409） |

## 恢复语义（对照需求）

- **启动即备份**：api 启动时后台执行 `pg_basebackup -X stream`（失败自动重试 10 次，
  状态见 `/backup`）；主库 WAL 由 `archive_command` 持续归档。
- **恢复点门控**：创建恢复点时记录其 LSN 并 `pg_switch_wal()`；后台任务持续校验
  「基准备份结束 LSN → 恢复点 LSN」之间每一个 WAL 段都已在归档目录中，齐全才置
  `ready`；未就绪的点发起恢复返回 409。早于基准备份重做起点的点标记 `expired`。
- **物理恢复**：每次恢复都 `cp` 基准备份到全新目录 `recovery_work/rN`，写
  `recovery.signal` 与 PITR 配置后启动独立 postgres——不是逻辑留史/影子表。
- **到达目标才报成功**：轮询副本直到 `pg_last_wal_replay_lsn() >= 目标 LSN`，
  并对业务表执行一次真实查询确认可查询，才置 `success` 并开放查询。
- **失败即报、不留残缺**：缺 WAL（`restore_command` 失败 → postgres FATAL）、
  恢复进程退出（pg_ctl 探活 + 后台看门狗每 3s 巡检）都会把任务置 `failed` 并记录
  postgres 日志尾部；失败任务的目录被清理，**不开放残缺副本，也不回退到上一次副本**
  （新恢复启动时会先退役并删除旧副本）。重试 = 再 POST 一次。
- **单一在途**：存在 running 任务时再发起返回 409。
- **不混数据**：新恢复启动即退役（停止+删除）旧副本并清空服务指针；每次恢复都是
  基准备份的全新拷贝，先后两个恢复点互不干扰。
- **主库不阻塞**：恢复只读归档卷与基准备份，对主库零写入、零锁。

## 环境变量（api 服务）

| 变量 | 默认 | 说明 |
|---|---|---|
| `PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE` | db/5432/postgres/postgres/traffic | 主库连接 |
| `REPLICA_PORT` | 55433 | 副本监听端口（容器内 127.0.0.1） |
| `RECOVERY_TIMEOUT_SEC` | 180 | 等待重放到目标点的超时 |
| `REPLICA_RUN_AS` | postgres | 副本进程运行用户（容器内 root 经 gosu 降权；本地裸跑可置空） |
| `BACKUP_ON_STARTUP` | 1 | 启动时是否做基准备份（预置备份的测试场景可置 0） |
| `INGEST_LOCK_TIMEOUT_MS` | 15000 | 同一路口补传批次互相等待的上限；超时返回 503 且不写入 |
| `WAL_ARCHIVE_DIR/BACKUP_DIR/RECOVERY_ROOT/STATE_DB` | /wal_archive /backups/base /recovery_work /state/api.db | 存储路径 |

## 测试

```bash
cd api
python -m pytest tests -q                       # 纯单元测试
TRAFFIC_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=traffic_test" \
  python -m pytest tests -q                     # 加上真实 PostgreSQL 集成测试
```

集成测试会 `TRUNCATE` 三张业务表，只能指向可丢弃的测试库。

## 已验证 / 未验证（诚实清单）

交付前在本沙箱用 PostgreSQL 16.16（PGDG 官方二进制）+ 本 API 真实运行验证。
**注意：本沙箱内核级禁止一切跨进程信号**（kill/pidfd 均 EPERM），导致环境性限制：
(1) `pg_basebackup` 的 checkpoint 信号被拒（`could not signal for checkpoint:
Permission denied`）；(2) 热备副本无法进入可连接状态（postmaster 收不到 PMSIGNAL）；
(3) PostgreSQL 无法唤醒锁等待者、`lock_timeout` 也不会触发。这些在正常 Linux /
Docker 中不存在，属沙箱特有。

已验证（PITR 部分为上一版基线交付时于本沙箱真实运行验证，本次未改动这些代码路径）：

- ✅ 主库建表、持续 WAL 归档（段 1..N 全部落归档卷）
- ✅ API 启动、/health、/docs、/openapi.json、/ 重定向（本次改动后重新验证）
- ✅ 备份失败路径：`pg_basebackup` 失败被捕获并记录到 `/backup`（含重试逻辑）
- ✅ 写入/误删演示接口（删除行数确定性符合预期）
- ✅ 恢复点创建 → pending →（WAL 齐）ready；归档连续性校验与 `pg_walfile_name` 交叉一致
- ✅ 未就绪恢复点发起恢复 → 409；不存在 → 404；早于备份的点 → expired
- ✅ **物理恢复正确性**：基准备份拷贝 + 归档 WAL + `recovery_target_name` 恢复到
  `before_delete`，用单用户模式在暂停点直接查询：**60 行全在（含被删的 10 分钟）、
  恢复点后的提交（IXB 6 行）为 0**；主库当时为 36 行且持续可写
- ✅ 缺 WAL 恢复 → postgres `FATAL: recovery ended before configured recovery
  target was reached` → 任务 failed（错误含日志）、副本不可查（409/404）
- ✅ 恢复进程退出 → 看门狗 3 秒内置 failed 并清理
- ✅ 只允许一项在途 → 并发提交返回 409
- ✅ 失败可重试（生成新任务、不服务旧数据）
- ✅ API 重启清理：遗留 running/success 任务置 failed、目录删除、指针清空
- ✅ LSN/WAL 段名计算单元测试、compose YAML、demo.sh 语法、OpenAPI 生成

乱序批量采集（本次新增，真实 PostgreSQL 16.16 + 本 API 运行验证）：

- ✅ 乱序到达：seq 1/3/4 中 3、4 持久为 `pending`，水位停在 1、`next_missing_seq=2`
- ✅ 缺口补齐：补 seq 2 后 2、3、4 按序发布，主表行与分钟一一对应
- ✅ 同分钟修订：已发布的同一分钟再收到高序号时覆盖旧值（21 → 99）
- ✅ 幂等重试：同 `(路口, 序号)` 同内容重传 `newly_stored=0`，台账与主表行数不变；
  同批次内完全重复的记录也被折叠
- ✅ 异内容整批拒绝：返回 409 并列出冲突明细，同批另一路口的观测与水位均未落库
- ✅ 同成同败：用触发器注入主表写入失败，观测台账与水位一起回滚（0 行）
- ✅ 持久性：API 重启/换进程后 pending 记录仍在，补齐后正常发布
- ✅ 水位不回退：已推进后再提交旧序号不会降低水位
- ✅ 同路口串行/异路口并行：`pg_try_advisory_xact_lock` 证明同路口锁互斥、不同路口
  锁键不同；锁被占用期间另一路口批次 0.02s 内正常完成
- ✅ 旧数据卷升级：只有 `traffic_minute_counts` 的库启动时自动补建台账表并可采集
- ✅ 校验：`seq=0`、负计数、非整分钟、空批次均返回 422
- ✅ 单元 + 集成测试共 24 项通过（`api/tests`，含跨发布分块边界的补齐用例）
- ✅ `/docs`、`/openapi.json` 含全部新接口；`scripts/ingest-demo.sh` 全流程通过；
  原有 `/demo/*`、`/primary/traffic`、`/demo/overview`、恢复点/恢复查询等接口行为不变

未能在本环境验证（沙箱限制所致，逻辑已在 docker 场景设计并走查）：

- ⚠️ `docker compose up` 整体构建与联调（沙箱无 docker）
- ⚠️ `pg_basebackup` 成功路径（沙箱禁止 checkpoint 信号；失败路径已验证）
- ⚠️ 热备副本接受连接后的 API 成功闭环（`status=success` 与 `/replica/traffic` 200）：
  沙箱中 postmaster 无法收到热备就绪信号。恢复本身已验证到达目标点（postgres 日志
  `recovery stopping at restore point` + `pausing at the end of recovery`），
  数据正确性已用单用户模式验证（见上）
- ⚠️ `pg_ctl stop` 退役旧副本（沙箱无法发信号；docker 中正常）
- ⚠️ 锁等待被唤醒与 `lock_timeout` 超时返回 503：两者都依赖 PostgreSQL 的跨进程信号，
  沙箱中被内核拒绝；只能验证「锁被占用时另一会话拿不到、不同路口不受影响」，
  无法在这里观察「释放后唤醒」和「超时」。这两条路径由 PostgreSQL 原生实现，
  并发正确性由单事务 + 每路口串行锁保证。

## 限制

- 单 api 进程、单副本端口：同一时刻一个恢复、一个副本（需求即如此）。
- 基准备份为启动时一份；api 重启会重取备份，此前创建的恢复点会标记 `expired`。
  采集台账在主库，不受 api 重启影响。
- 恢复点名称唯一（重复创建返回 409）；WAL 归档卷只增不减，演示规模无清理策略。
- 采集台账只增不删（`pending` 与已发布观测都保留），演示规模无归档/清理策略。
- 无认证鉴权，仅限内网演示。
