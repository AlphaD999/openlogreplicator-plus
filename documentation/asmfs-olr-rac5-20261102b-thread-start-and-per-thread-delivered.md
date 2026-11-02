# 2026-11-02b：归档清单漏掉整个 redo 线程 + 逐线程 delivered-scn（开关，默认关）

本版换 `bin/OpenLogReplicator` 一枚。**配置一行不改**（新开关默认 0 = 旧行为），checkpoint 新增一个键
（旧文件照读），可独立回滚到 20261102a 或 20261030f。

基线：`asmfs-olr-rac5-delivery-20261102a`（二进制 `d785bfe7…`）
本版：84,005,600 B，sha256 `cee0a3959f7e7d002c4d881707b1951db20fa4b3cf65d3628775cc71c68c80fc`

---

## 1 归档清单漏掉整个 redo 线程（新发现，静默丢数）

**现象**（podman 复现台，2026-11-02）：OLR 只起 thread 1，thread 2 的 45 份归档**一份都没读**，
日志里连一行相关记录都没有。用装置自带二进制（20261030f 那条线）A/B 复现同样的行为，确认不是 20261102a
引入的。

**根因链**（三步，前两步各自都"合理"）：

1. `positionReader()` 的 SQL 用 `FIRST_CHANGE# - 1 <= :start-scn` 只挑"**覆盖**起点 SCN 的归档"。
   该装置 `start-scn = 42547587`，thread 1 最老归档 `FIRST_CHANGE# = 42547587`（覆盖），
   thread 2 最老归档 `42547590`（`42547589 <= 42547587` 不成立）⇒ SQL 只回一行：thread 1。
2. `metadata->threadPos` 里因此没有 thread 2，`threadSequence(2)` 退化为 `Seq::zero()`。
3. `archGetLogList()` 绑的是"**已知线程**的最小期望 sequence"（本例 thread 1 的 20041）：
   `WHERE SEQUENCE# >= 20041` ⇒ thread 2 的 15320..15364 全部被过滤掉。**该线程从此不可见，且无告警。**

**影响面**：任何"从某 SCN 起"的定位（`start-scn`、checkpoint 丢失后按 SCN 重新定位、`start-time`），
只要某个实例最老的归档晚于该 SCN，那个实例的 redo 就**永久不投递**。不是 10-30 那次停机的成因，
但同属"静默丢数"，且在 POC/切换场景很容易踩到（起点 SCN 随手取，比最早归档早或晚几秒都行）。

**修法**：给两条定位 SQL 各加第三个 `UNION` 分支——"该线程最老的归档"，外层仍是 `MAX(SEQUENCE#)`：

* 覆盖 SCN 的分支存在时，`MAX(覆盖) >= MIN(全部)`，答案与旧版**完全一致**；
* 不存在时，取该线程最老的归档，即"该线程第一个晚于起点的归档"，语义正确且代价最小。

新增第 4 个 bind（`RESETLOGS_ID`），trace 里多打一行 `PARAM4`。

**装置实测**：

| | 旧 SQL | 新 SQL |
|---|---|---|
| 手工执行 | `thread=1 seq=20041` | `thread=1 seq=20041` + `thread=2 seq=15320` |
| OLR 日志 | 只有 `starting sequence for thread: 1` | `starting sequence for thread: 1 is: 20041` + `for thread: 2 is: 15320` |
| 解析 | 46 趟，全是 thread 1 | **111 趟，含 thread 2 的 15320/15321/...**，0 ERROR |

## 2 逐线程 delivered-scn（`state."drop-delivered-per-thread"`，默认 0）

**现象**：2026-10-30 现场 10 小时 `restart_reread_drops = 48,003,797`，其中启动后 **4 分钟就 997,907**；
而那次进程是全新启动、`checkpoint` 目录为空（启动即 `10003`），**根本不存在"重启回退窗口"**。

**机制**：丢弃判据用的是**全局最大已投递 SCN**（`Ctx::deliveredUpToScn`），而 `60045`（no-work / timeout）
与 `unaligned` 的设计目的就是**放弃全局 SCN 顺序**去投递迟到线程。迟到的归档（安静实例的 redo 晚到）
里面所有 ≤ 全局水位的提交都被当成"已投递"直接丢——虽然从没投过。理论上与"重读去重"无法区分，
实测量级对不上（同期只有一个 `60058`，复读字节恒为 929 MB，解释不了 4800 万条）。

**改动**：

| 位置 | 内容 |
|---|---|
| `Ctx` | 新增 `deliveredUpToScnByThread[16]`（逐线程水位）、`threadReleased[16]`（该线程是否曾被释放）、开关 `dropDeliveredPerThread`，以及唯一判据入口 `deliveredDropLimit(thread)` |
| `DeliveryExecutor::commitCommon` | 丢弃比较改用 `deliveredDropLimit(transaction->thread)`；交付时同时推进全局与逐线程水位 |
| `CommitSorter` | 三条释放路径（`60045 no-work`、`60045 timeout`、`unaligned`）置 `threadReleased` 标记，**进程内粘住**（被释放过的线程随时可能再乱序） |
| checkpoint 写 | 新增 `"delivered-scn-by-thread":[{"thread":N,"scn":X},…]`，**只在开关打开时写**——`Ctx::checkJsonFields()` 对未知键抛 `20003` 且该检查默认开启，若每次都写，回滚到 20261102a / 20261030f 的二进制会在启动读 checkpoint 时直接失败 |
| checkpoint 读 | 按线程回填；文件里没有这个键（旧 checkpoint）时所有线程水位为 0 = **不丢弃**（保守方向） |
| 新告警 `60069` | 开关打开但 checkpoint 无该数组时一次性提示"本轮不丢弃，回退窗口会重投"；见 `documentation/log-messages/6.warning-messages.adoc` |

开关关闭时（默认）判据与旧版逐字节一致；开关打开时规则是：**只与"该提交所属线程"的水位比较，
且该线程从未被释放过才丢**。

**装置实测（2 线程）**：

| 判据 | 结果 |
|---|---|
| 启动行 | `a commit is dropped as already delivered by the watermark of its own redo thread ... ("drop-delivered-per-thread": 1)` |
| checkpoint（开关 on） | `"delivered-scn":42548932` + `"delivered-scn-by-thread":[{"thread":1,"scn":42548845},{"thread":2,"scn":42548932}]` |
| 重启恢复 | `delivered up to scn: 42606766 for thread: 1` / `42606588 for thread: 2`（逐线程值不同，正是要点） |
| **checkpoint（开关 off）** | 21 份 checkpoint 中 **0 份**含 `delivered-scn-by-thread`，全局 `delivered-scn` 照旧——回滚不会踩 20003 |
| `60069` | 0（数组存在） |
| 停机 | `SIGTERM` 2 s 退出，`10014=0`，0 ERROR |

**先观察、后打开**：本版默认关。建议上线后先看 24 小时的 `60045` 频率与 `restart_reread_drops`
（用 `trace: TRANSACTION` 抽 20~30 条 `dropping a commit delivered before the restart` 与 Kafka/Doris 对账），
确认后再置 1。

## 3 顺带验证（20261102a 的能力，第一次在 2 线程形态下跑）

thread 2 起来之后，转储里出现 **`SRC-reader-arch-1` 与 `SRC-reader-arch-2` 两个实例**——
20261102a 的"每线程一个归档 Reader"第一次被真实验证（此前装置只有 thread 1，只能验证单实例）。

## 4 未做（仍然）

1. 多 reader **并行读**（parser 仍是单线程；reader 只是被它按趟驱动）；
2. `60010` 限流（一次 60058 重读仍会在十几秒内产出两千多行）；
3. `Ctx::watchdogProgressUt/Thread` 仍是全局（`60055` 的 "no progress for: 0 s" 会继续出现，`reader:` 段已按线程）；
4. `kill -USR1` 的线程转储在 `mainThread != pthread_self()` 时直接返回，容器里基本打不出来；
5. 读缓冲预算未按线程切分（`read-buffer-max-mb` 仍是全局预算）；
6. `drop-delivered-per-thread` 的对账（见 §2 末）——打开前必须做。

## 5 构建

```bash
cd /opt/olr-build23/build
/usr/bin/cmake3 .            # 必须先重跑：否则 compiled 戳是上一次配置时刻
make -j2 OpenLogReplicator
```

| 门槛 | 结果 |
|---|---|
| 尺寸 / sha256 | 84,005,600 B / `cee0a3959f7e7d002c4d881707b1951db20fa4b3cf65d3628775cc71c68c80fc` |
| `--version` 第 2 行 | `compiled: 2026-11-02 06:56`(UTC, 本地 14:56) |
| GLIBC 上限 | `GLIBC_2.12` |
| NEEDED 中的 rdkafka/zstd/prometheus | 0 |
| 新增告警 | 0（编译日志里只有既有的 `unused variable '_'` 结构绑定告警） |
| cfgcheck | `/root/olr-zjsfrpt.json`、`/root/olr-zjpvsrpt.json`：`20003/30001/12001` 各 0 |

发布：`/opt/dist/OpenLogReplicator` + `.sha256`；`d785bfe7-20261102a` 与 `cee0a395-20261102b` 均在
`/opt/dist/archive/`。

交付包：`asmfs-olr-rac5-delivery-20261102b`，61 文件 / 95,405,941 B，zip 31,105,520 B，
sha256 `492e9f71d768d3aaa44a8d3bb23dce662e1482625996757b0172955966527421`（包内 `bin/SHA256SUMS` 已自检）。

> 说明：装置端到端用的是同一份源码的 `84005416`（仅 `compiled` 字符串不同）——构建时间戳变化不改变
> 任何代码路径，交付件 `db08e10f` 与它逻辑等价。

## 6 验收判据

| 看什么 | 期望 | 不符合说明什么 |
|---|---|---|
| `starting sequence for thread: N` 行数 | == redo 线程数（生产 4） | SQL 修复未生效（某线程仍无位置） |
| 线程转储 `-reader-arch-N` 条数 | == redo 线程数 | 每线程 Reader 未生效 |
| checkpoint 文件 | 含 `delivered-scn-by-thread` | 写侧未生效 |
| 启动行 | 打印 `drop-delivered-per-thread` 的生效口径 | 配置键未识别（应在 `state` 白名单里） |
| `60069` | 只在旧 checkpoint + 开关打开时出现一次 | — |
| `restart_reread_drops` | 开关打开后与 `delivered_per_s` 同量级，不再单独暴涨 | 仍有迟到线程被丢弃 |
| 回归 | `50049=0`、`60058` 正常、`10014=0`、`60053=0` | — |

## 7 回滚

```bash
cp -p /opt/olr/OpenLogReplicator /opt/olr/OpenLogReplicator.bak-cee0a395
cp -p /opt/dist/archive/OpenLogReplicator-d785bfe7-20261102a /opt/olr/OpenLogReplicator
cd /opt/olr && printf '%s  OpenLogReplicator\n' d785bfe782664b6f94b3a707727299c2e5d5534a833efce8dbc84006cc03c1aa > SHA256SUMS.tmp
grep -v ' OpenLogReplicator$' SHA256SUMS >> SHA256SUMS.tmp 2>/dev/null; sort -u SHA256SUMS.tmp > SHA256SUMS; rm -f SHA256SUMS.tmp
sha256sum -c SHA256SUMS
service olr-zjsfrpt stop; service olr-zjsfrpt start
```

**回滚前检查 checkpoint**：`delivered-scn-by-thread` 是旧二进制的未知键，而 `Ctx::checkJsonFields()` 对未知键
抛 `20003`（`JSON_TAGS` 检查默认开启）。本版只在开关打开时才写这个键，所以：

* 若本版是**默认配置（开关关）**跑的：checkpoint 里没有该键，**直接回滚即可**；
* 若本版曾**打开过开关**：先在配置里把 `drop-delivered-per-thread` 置回 0 并等下一份 checkpoint 写出
  （或人工把 `"delivered-scn-by-thread":[...]` 从 checkpoint 文件里删掉），再回滚。
