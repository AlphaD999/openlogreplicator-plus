# OpenLogReplicator 支持 Oracle RAC(+ASM 归档旁路)开发方案

**版本**: v1.1
**状态**: 已实施(代码完成,待 Linux 环境构建与 RAC 集成验证)
**日期**: 2026-09-08
**基线**: OpenLogReplicator 2.0.0
**关联决策**: 归档日志通过 `log_archive_dest_2` 写入普通文件系统;本期不做 ASM 原生直读与在线日志实时读取

> **实施记录(2026-09-08)**: T0-T7 代码改造已完成。关键实现与计划的差异:
> - T6 CommitSorter 落地为 `src/parser/CommitSorter.h/.cpp`,挂载于 Metadata(指针)并由 Parser 实现 Executor;单实例默认启用排序器(水位=当前 LWN SCN,行为等价、输出顺序不变);
> - T2 RAC 检测使用 `cluster_database` 参数(未叠加 V$THREAD 行数判定,简化);FS 归档可达性校验 SQL 见 `SQL_CHECK_FS_ARCHIVED_LOG`;
> - 错误码实际分配:10073(RAC 无 FS 归档)、60038(closed thread 尾段缺失等待);方案中占位的 10069/60029 已被其他消息占用;
> - T8 验证状态:静态人工审查通过;Windows 开发机无 Linux 工具链(g++/cmake/WSL 均不可用),待 Linux 环境执行全量构建 + ORC-CORE 回归 + 双节点 RAC 集成测试。

---

## 1. 背景与目标

当前 OpenLogReplicator 仅支持单实例、非 ASM 源库(限制声明见 `documentation/installation/installation.adoc:40,45`)。本方案的目标:支持 Oracle RAC 源库,redo/归档日志存放于 ASM,OLR 只从普通文件系统读取归档日志副本。

成功标准:

1. RAC 所有实例(thread)的事务均被捕获,无丢失;
2. 输出事件跨 thread 按提交 SCN 全局有序;
3. checkpoint 重启恢复无空洞、无重复;
4. 单实例场景行为与性能零回归(默认路径不改变)。

---

## 2. 决策记录(ADR)

| 编号 | 决策 | 理由 | 后果 |
|---|---|---|---|
| D1 | ASM 采用 FS 归档旁路(`log_archive_dest_2`),不实现原生 ASM Reader | 原生 ASM 直读 = 自研只读 ASMFS,工作量与风险不可控 | 复制延迟 = 归档切换周期;在线日志不可读,自动 archives-only |
| D2 | 检测到 RAC 后自动进入 archives-only 模式 | 在线日志在 ASM 中无法从外部访问;避免误配置导致启动失败或静默漏数 | 需 DBA 侧周期性归档切换(每个节点),文档化 |
| D3 | 跨 thread 全局有序采用"提交事件排序器 + 低水位冲刷",不改造 Parser 解析循环 | OLR 是事务整单输出模型(`Transaction::flush`,`src/parser/Transaction.cpp:199`),事务重做全部产生于单一 thread;只有 commit/DDL/checkpoint 事件需要排序 | 新增独立组件 CommitSorter;解析保持单线程;输出延迟受慢 thread 影响(业界同类工具固有) |
| D4 | 检查点状态文件向后兼容:无 `threads` 字段的旧文件按 thread=1 解析 | 支持老版本平滑升级、不丢位点 | 序列化增加新字段,反序列化保留双路径 |
| D5 | 归档副本选择在 SQL 层过滤 `+` 前缀(ASM)路径 | V$ARCHIVED_LOG 对同一 sequence 返回多个 DEST 行,ASM 行可能先被打开而失败(现行为:重试 `archReadTries` 次后抛 10009) | 数据库侧保留 ASM 归档目的地不受影响 |
| D6 | 事务 map key 纳入 thread | RAC 各实例 undo usn 空间独立,XID 存在跨 thread 碰撞可能 | `TransactionBuffer` 两处 key 生成修改 |

---

## 3. 现状:单实例假设清单(改造依据)

| # | 假设 | 位置 | 影响 |
|---|---|---|---|
| S1 | 全局唯一 sequence,日志流串行 | `src/metadata/Metadata.h:115-127` 单一 sequence/fileOffset/checkpoint*;`src/replicator/Replicator.cpp:622-624,733-748,798` | RAC 每 thread sequence 独立、互相重叠 |
| S2 | RedoLog 无 thread 维度 | `src/metadata/RedoLog.h:30-35` | 无法 (thread,group,seq) 定位 |
| S3 | 归档 SQL 不区分 thread | `src/replicator/ReplicatorOnline.h:34-51`(V$ARCHIVED_LOG 无 THREAD#/排序键错)、`:121-138`(SCN→MAX(SEQUENCE#)) | 起点定位与续读错乱 |
| S4 | 在线日志按 group 全量加载 | `src/replicator/ReplicatorOnline.cpp:1522-1563`;`Replicator.cpp:301-336` | RAC 下 group 关联 thread,含 ASM 成员时打开失败 |
| S5 | 文件顺序 = SCN 顺序,解析即交付 | `src/parser/Parser.cpp:1516,1564,1576`(checkpoint)、`:551-555` → `appendToTransactionCommit:792` | 跨 thread 文件交错时提交乱序 |
| S6 | 事务 key = (xid, conId) | `src/parser/TransactionBuffer.cpp:56-93` | 跨 thread XID 碰撞 |
| S7 | 恢复位置单值 | `src/parser/Parser.cpp:1290-1299,1586`;`src/metadata/SerializerJson.cpp:614-622` | 无法逐 thread 恢复 |
| S8 | 缺失归档 = 无限等待 | `src/replicator/Replicator.cpp:739-748`(60027) | RAC 实例 down(thread closed)时永久卡死 |

---

## 4. 目标架构

```
Oracle RAC (ASM)
   ├─ 节点1 redo thread=1 ─┐ 归档(ASM dest) + 归档(FS dest via log_archive_dest_2)
   ├─ 节点2 redo thread=2 ─┤        │
   └─ ...                  ─┘        ▼
                        OLR(单进程,连接任一实例)
    ┌──────────────────────────────────────────────────────┐
    │ 发现层   RAC 检测 → 自动 archives-only               │
    │          per-thread 归档发现(THREAD#/过滤 + 前缀)     │
    │ 调度层   per-thread 游标 (thread → 期望 seq)          │
    │          thread open/closed 状态机(封口/等待)         │
    │ 解析层   Parser 逐文件串行解析(现有逻辑不变)           │
    │          事务缓冲 TransactionBuffer(key 含 thread)     │
    │ 交付层   CommitSorter:按 commit-SCN 排序              │
    │          低水位 = min(active thread 已解析位置)        │
    │          → 冲刷 → Builder → Writer(现有链路不变)       │
    │ 状态层   checkpoint 状态 = per-thread 位置 + 冲刷水位   │
    └──────────────────────────────────────────────────────┘
```

关键设计:解析可以按任何 thread 文件顺序进行(每 thread 内有序即可),**只有交付需要全局有序**,由 OLR 的事务整单输出模型天然支撑(事务数据在 commit 前全部缓冲于 swapped memory)。该机制等价于 LogMiner 的 low-water-mark 模型。

---

## 5. 任务分解

### T0 — 审计:乱序敏感事件清单(0.5 d,无代码)

- 产出"必须排序事件表",覆盖:
  - commit 触发:`src/parser/Parser.cpp:551-555` 及 `appendToTransactionCommit`(`:792`)内部 flush 链;
  - DDL:`src/builder/Builder.h:1261 processDdl`;
  - 系统事务 schema 更新:`src/parser/Transaction.cpp:219-224,560-568`;
  - checkpoint 事件三触发点:`Parser.cpp:1516 / 1564 / 1576`。
- 交付物:该表作为 T6 的验收依据。

### T1 — 位置状态 thread 化(2-3 d,硬前置)

- `src/metadata/RedoLog.h`:新增 `uint16_t thread`;比较运算符改 (thread, group, path);同步所有 `new RedoLog(...)` 调用点(`ReplicatorOnline.cpp:1553`、`SerializerJson.cpp:96-118`、离线配置解析)。
- `src/metadata/Metadata.h/.cpp`:
  - 新增 `struct ThreadPos { Seq sequence; FileOffset fileOffset; Scn firstScn, nextScn; bool closed; }` 与 `std::map<uint16_t, ThreadPos> threadPos`;
  - `setSeqFileOffset(Seq, FileOffset)`、`setNextSequence()`、`setFirstNextScn` 增加 thread 参数(单实例调用点默认 thread=1,保持默认路径零改动);
  - 现有 `sequence/fileOffset/checkpointSequence` 等公开字段保留为"主 thread(1)视图",内部重定向。
- `src/metadata/SerializerJson.cpp`:序列化增加 `"threads":[{...}]`(`:56-67` 附近);反序列化(`:614-622`)兼容旧格式。
- `src/metadata/Checkpoint.cpp`、`src/parser/Parser.cpp:1290-1299,1586`:恢复与推进位置改走 per-thread。
- `scripts/gencfg.sql:256-279`:V$LOGFILE join V$LOG 输出 THREAD#,`online-redo` JSON 增加 thread 字段。
- **验收**:单实例全量回归零差异;旧状态文件可加载;新状态文件含 threads。

### T2 — RAC 检测与模式切换(1 d)

- `src/replicator/ReplicatorOnline.cpp` `loadDatabaseMetadata`(`:84-239`):复用 `SQL_GET_PARAMETER` 读取 `cluster_database`;为 TRUE 或 V$THREAD 行数 > 1 判定为 RAC。
- RAC 行为:
  - info 日志明示模式;内部置 `REDO_FLAGS::ARCH_ONLY`(语义见 `Replicator.cpp:171-172,248`),跳过在线日志路径;
  - 校验 FS 归档可达:V$ARCHIVED_LOG 存在非 `+` 前缀 NAME,否则抛出新错误码并提示配置 `log_archive_dest_2`;
  - 用户显式 `start-seq` 等参数按主 thread 语义校验并给出 RAC 提示。
- **验收**:RAC 库启动自动跳过在线读取;单实例库行为不变。

### T3 — 归档发现与副本选择(2 d)

- `src/replicator/ReplicatorOnline.h`:
  - `SQL_GET_ARCHIVE_LOG_LIST`(`:34-51`):增加 `THREAD#`、`AND NAME NOT LIKE '+%'`、`ORDER BY THREAD#, SEQUENCE#, DEST_ID`;单实例走同一 SQL 天然兼容(thread=1);
  - `SQL_GET_SEQUENCE_FROM_SCN`(`:121-138`):per-thread 查询与定位;
  - 新增 `SQL_GET_THREAD_INFO`(V$THREAD status/enabled + V$LOG),供调度层使用。
- `src/replicator/ReplicatorOnline.cpp` `archGetLogOnline`(`:1569-1614`):Parser 携带 thread;
- `src/replicator/Replicator.cpp` `archGetLogPath`(`:456-538`)与 `getSequenceFromFileName`(`:347-409`):解析 `%t` 并返回 thread(现已被丢弃)。
- **验收**:RAC 下仅 FS 副本入队;任一 thread 归档可被发现。

### T4 — per-thread 归档调度与 thread 状态机(3-4 d)

- `src/replicator/Replicator.cpp` `processArchivedRedoLogs`(`:690-816`):`archiveRedoQueue` 排序键改 (thread, seq)(`parserCompare`, `:622-624`);推进 per-thread 期望 seq。
- 缺口语义修正(S8):active thread → 缺失 = 未归档,等待(保留 60027);thread closed → 封口游标;若 closed 前尾段缺失(实例 crash 后由 peer recovery 延迟归档)→ 等待 + 限时告警(新错误码),不永久静默。
- `updateResetlogs`(`:626-661`)、`positionReader`(`ReplicatorOnline.cpp:241-334`)、`updateOnlineRedoLogData`(`ReplicatorOnline.cpp:1446-1567`,RAC 模式跳过在线部分仅刷新 thread 状态)同步 per-thread。
- **验收**:双实例日志全部消费;kill 实例后不卡死、恢复后继续;thread 内无缺口跳过。

### T5 — 事务身份纳入 thread(1 d)

- `src/parser/TransactionBuffer.cpp:56-93`:`findTransaction/dropTransaction` 的 `xidMap` 生成公式纳入 thread;`checkpoint()`(`:268-273`)min 语义同步。
- 审计 `src/parser/Parser.cpp` 全部 `findTransaction` 调用点。
- **验收**:跨 thread 同形 XID 不冲突(构造两 thread 相同 usn/slt/sqn 的事务测试)。

### T6 — CommitSorter 提交排序器 + 低水位冲刷(4-6 d,正确性核心)

- 新增 `src/parser/CommitSorter.h/.cpp`,加入构建(`src/CMakeLists.txt`):
  - 接口:`enqueueCommit(commitScn, transaction*)`、`enqueueEvent(scn, event)`、`setThreadWatermark(thread, scn)`、`flushReady(builder, metadata)`;
  - 低水位 = min over active threads 已解析 scn(closed 计入终值后退出水位);
  - 事件按 scn 升序冲刷;commit 事件触发现有 `Transaction::flush` 链。
- 挂载点(只改触发时机,不改内部):
  - `Parser.cpp:551-555` commit 分支:原 `appendToTransactionCommit` 拆为"登记待提交事务入排序器"与"冲刷期执行原逻辑"两段(内部逻辑原样保留在 flush 路径);
  - `Parser.cpp:1516/1564/1576` checkpoint:改为入队排序器,随水位冲刷;
  - DDL / 系统事务按 T0 事件表逐项挂载。
- checkpoint/状态对齐:OLR 持久化的"已确认位置"一律取**冲刷水位**而非解析水位(防止恢复后空洞);`Parser.cpp:1586`、`Metadata::checkpoint`、`stopLogSwitches` 语义同步。
- **验收**:双实例高并发提交,输出严格按 commit-SCN 有序;提交后立即重启,恢复无空洞无重复;单实例路径下排序器空转零行为差异。

### T7 — 配置、错误码与文档(1-2 d)

- 配置:新增可选 `source[].reader."archives-only"`(显式覆盖自动检测)等字段,**必须同步登记 `readerNames` 白名单**(`src/OpenLogReplicator.cpp:496-515`),并注册解析与互斥校验;flags 位域上限 524287 如需新增位需同步扩容校验(`:519`)。
- 错误码:新增 RAC 相关错误码,登记 `documentation/log-messages/3.configuration-error-messages.adoc`、`1.runtume-error-messages.adoc`。
- 文档:`documentation/installation/installation.adoc:40-45`(支持范围改写 + "RAC 部署"小节:log_archive_dest_2、每节点归档切换、延迟说明)、`documentation/user-manual/6.replication.adoc`(RAC 语义)、示例 JSON 注释。
- **验收**:字段白名单/范围检查通过;文档与代码行为一致。

### T8 — 测试与验证(持续,收尾 3-5 d)

- 单元:ThreadPos 序列化兼容、XID key、CommitSorter 顺序/水位。
- 集成(两节点 RAC + ASM + FS 归档):并发写提交序、kill/重启节点、recovery 延迟归档、resetlogs、checkpoint 恢复、归档缺口、慢 thread 输出延迟观察。
- 回归:单实例全量 ORC-CORE 套件零回归;性能对照(排序器空转开销 < 阈值)。

---

## 6. 数据结构与状态格式变更

checkpoint JSON 示意:

```json
// 旧格式(单实例,读取兼容)
{ "scn": 123, "resetlogs": 1, "seq": 5, "offset": 4096, ... }

// 新格式(RAC)
{ "scn": 123, "resetlogs": 1,
  "threads": [
    { "thread": 1, "seq": 5,   "offset": 4096 },
    { "thread": 2, "seq": 12,  "offset": 8192 }
  ], ... }
```

兼容规则:反序列化遇旧格式 → 生成 `threads=[{thread:1, seq, offset}]`;`min-tran` 收敛为各 thread 项内最小(冲刷水位)。

---

## 7. 里程碑与依赖

```
T0 ─> T1 ─> T2 ─> T3 ─┐
            │          ├─> T4 ─> T6 ─> T7 ─> T8
            └──── T5 ──┘        (T6 依赖 T1,T4,T5)
```

- **M1** = T0 + T1 + T5:thread 化数据模型(默认路径零行为差异,可独立合入)。
- **M2** = T2 + T3 + T4:RAC 全 thread 归档可消费(thread 内正确,联调态)。
- **M3** = T6:全局有序交付 + 低水位 checkpoint,功能完成。
- **M4** = T7 + T8:配置/文档/验证收尾。

---

## 8. 风险登记册

| 风险 | 等级 | 缓解 |
|---|---|---|
| 排序器冲刷语义导致 checkpoint 空洞/重复 | 高 | 只认冲刷水位持久化;T0 事件表逐项审计;M3 专用测试 |
| Parser/事务回归 | 高 | 不触碰 `Parser::parse()` 主循环与 `Transaction::flush` 内部;仅改触发时机;全量回归 |
| ASM 副本被优先打开导致 10009 | 中 | T3 SQL 层过滤 `+` 前缀 + `archGetLogOnline` 前缀断言双保险 |
| 实例 down 等待语义误判(丢数/卡死) | 中 | T4 三态机(active 等待 / closed 封口 / closed+尾段缺 = 告警限时) |
| 状态文件升级兼容 | 中 | T1 默认回退 thread=1 + 兼容用例 |
| 慢 thread 拖慢全局输出 | 低 | 文档化延迟模型;closed 退出水位;排序器水位可调阈值预留 |
| 配置白名单漏登记导致启动失败 | 低 | T7 集中处理 + 配置测试 |

---

## 9. 不在本期范围

- ASM 原生直读、在线日志实时读取(后续单独立项);
- 多 Parser 并行解析(CommitSorter 接口预留扩展点);
- RAC 下 Data Guard 备库特殊处理(沿用现有 standby 路径)。

---

## 10. 附录:关键代码位置索引

| 主题 | 位置 |
|---|---|
| 状态字段 | `src/metadata/Metadata.h:115-127`、`src/metadata/SerializerJson.cpp:56-67,614-622` |
| RedoLog | `src/metadata/RedoLog.h:30-35` |
| 归档 SQL | `src/replicator/ReplicatorOnline.h:34-51,121-138,159-172` |
| 归档消费 | `src/replicator/Replicator.cpp:690-816,622-624` |
| 在线逻辑(禁用处) | `src/replicator/Replicator.cpp:301-336,818-922`;`src/replicator/ReplicatorOnline.cpp:1446-1567` |
| commit/checkpoint 触发 | `src/parser/Parser.cpp:551-555,792,1516,1564,1576` |
| 事务缓冲 flush | `src/parser/Transaction.cpp:199-569` |
| 事务 key | `src/parser/TransactionBuffer.cpp:56-93,268-273` |
| 配置白名单 | `src/OpenLogReplicator.cpp:468-515,517-531` |
| 文档限制声明 | `documentation/installation/installation.adoc:40-45` |

---

*本文档为开发计划文档,随实现进展持续更新;任务验收标准以 T0-T8 各节为准。*
