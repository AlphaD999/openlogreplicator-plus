# 2026-11-02a：每线程归档 Reader（真正隔离）+ 窗口三位置一起动

本版换 `bin/OpenLogReplicator` 一枚。**配置一行不改，checkpoint 结构与 20261030f 相同，可独立回滚。**

基线：`asmfs-olr-rac5-delivery-20261030f`（二进制 `66443011…`）
本版：`OpenLogReplicator` 83,905,896 B，sha256 `d785bfe782664b6f94b3a707727299c2e5d5534a833efce8dbc84006cc03c1aa`

---

## 1 现场故障复盘（2026-10-30 20:26:29）

部署件是 **20261030f / `66443011`**（已含 `5b24012`：`setFileName` 同名不清零 size + `slice-resume` 前 `stat` 校验）。

> 注意：启动行里的 `compiled: 2026-10-29 05:46` **不是编译时刻**，是 `CMakeLists.txt:23` 的
> `string(TIMESTAMP … UTC)`，即 **cmake 配置时刻**。`eb13dcde`（20261029e）与 `66443011`（20261030f）
> 用的是同一个 build 目录，两枚打的是同一个戳。本版起出件必须 **重跑 cmake**（见 §8）。

日志（`/opt/olr/olr-zjsfrpt.log`，10:25:55 启动 → 20:26:29 停机）末段：

```
20:23:10  processing redo log: thread 2 seq 965484 offset 1024
20:23:36  FINISHED (sliced)  position 473,235,456 / 2,483,411,968
20:23:37  continuing on the open handle at 474,742,784 ("slice-resume")
20:24:12  FINISHED (sliced) 928,382,976 / 2,483,411,968
20:24:13  60024 gate: scan: 945,422,336 fileSize: 0 fileSizeStat: 0 gateSize: 0 nearEnd: no attempts: 1
20:24:13  continuing on the open handle at 929,431,552
20:24:13  60058 保存位置(block 1815301) 不是 lwn 头 → 整档从头重读 929 MB
20:24:31~44  60010 风暴：13 秒 2,338 条（重读丢掉了已提交事务，后续 rollback 记录找不到事务）
20:26:25  FINISHED (sliced) 929,431,552 / 2,483,411,968      ← 同一位置、同一假大小
20:26:29  continuing on the open handle at 931,528,704
20:26:29  processing redo log: offset 931,553,280             ← = resume 偏移 + 24,576
20:26:29  ERROR 50049 invalid lwn scn: 11202083500292
          (block: 1819440, lwnEndBlock: 1294156593, passStartBlock: 1819440, resumed: yes)
20:26:29  Replicator for: ZJSF is shutting down                ← 50049 在 Replicator 里是致命的
20:26:29  Kafka producer exit code: 0
20:26:29  ERROR 10014 stacktrace for thread: 139841718186592   ← 收尾段又一次信号
```

三条判据说明这**不是**"归档被换掉/读坏了"：

1. `5b24012` 的 `stat` 闸门当时是通过的（`published == st_size`），续读是被允许的；
2. 坏块位置 `1819440 × 512 = 931,553,280`，正好是 resume 偏移 + 24,576，即**同一个 64 KB chunk 内**、
   而该 chunk 当时只读进了开头 512 字节；
3. `lwnScn = 11,202,083,500,292`、`lwnEndBlock = 1,294,156,593`（折合 662 GB 偏移）都是随机值，
   符合"未初始化内存"而不是"磁盘上的坏块"（真坏块会先撞 `40002`/`60024` 的 block/sequence/CRC 校验）。

---

## 2 四个缺陷

### P0-1 停机段重复释放 → `10014`

`readerDropAll()` 先删 `readers` 里的全部对象，再删 `archReaders` 里的全部对象；而归档 Reader 是
`readerCreate()` 创建的，**同时**在 `readers` 里（`readers.insert()`）→ 每个对象被删两次。
更糟的是 `archReaderFor(thread)` 调的是 `readerCreate(0)`，而该函数**按 redo group 去重**（归档 group 恒为 0），
四个 slot 指向同一个对象 → 同一对象被删 5 次。

证据：线程转储里只有一条 `SRC_ZJSF-reader-0`；`10014` 恰好出现在 `Kafka producer exit code: 0` 之后。

### P0-2 "每线程一个 Reader"从未生效

同上 `readerCreate(0)` 的去重。`fileName / fileSize / sequence / bufferStart / bufferEnd /
sliceResume*` 全部共享，直接导致：

- `60055` 里 `(thread: 3)` 配 `file=thread_2_seq_965484`（快照当时是单份全局的，本版一并按线程）；
- `5b24012` 的"同名不清零 size"早退条件失效：**别的线程一次 pass 就把 published 清成 0**
  → `gateSize = 0` → `nearEnd = no` → 中段 `STOPPED` → 整档 929 MB 重读（20:24:13 那次）；
- `slice-resume` 复用 handle 的前提（"这个 fd 是这份归档的"）不成立。

### P0-3 窗口记账（真正的停机根因）

`resumeAt()` 在 `48bdeca` 里**立刻把 reader 置为 `STATUS::READ` 并唤醒**，而随后 `Parser::parse()`
用 `setBufferStartEnd(lwnConfirmedBlock, lwnConfirmedBlock)` 只抬 `bufferStart/bufferEnd`、**不动 `bufferScan`**：

```
resumeAt():  start=end=scan=aligned, STATUS::READ   → 读循环从 aligned 开始读
reader:      bufferEnd = aligned + 512             → 只读了 512 字节
parse():     bufferEnd = parserPos (= aligned+24576), scan 仍停在 aligned+512
reader:      下一次 read1 → bufferEnd 追到 parserPos+512
parser:      判据 confirmedBufferStart < bufferEnd 成立 → 解码 parserPos 处 chunk 内未读内存
             → 随机字节里 vld&0x04 恰好为 1 → lwnSize/lwnScn 垃圾 → 50049 → stopHard
```

`vld&0x04` 为 0 时就是 60058（20:24:13 那次）。**同一个 bug 的两种"运气"。**

### P0-4 10016 原本是致命的

`read1/read2` 拿不到 chunk 时 `ret = REDO_CODE::ERROR` → `processArchivedRedoLogs` 不在
`skip-unreadable-archive` 白名单里 → `RuntimeException(10047)` → 停机。
单 Reader 时"检查到分配"之间没人抢，永远不会空；一线程一 Reader 后这是必须处理的竞态。

---

## 3 本版改动

| # | 文件 | 改动 |
|---|---|---|
| 1 | `replicator/Replicator.{h,cpp}` | 新增 `readerCreateArch(thread)`：**不复用、不进 `readers`**，线程名 `-reader-arch-<thread>`；`archReaderFor()` 改调它 |
| 2 | `replicator/Replicator.cpp` | `readerDropAll()`：两个循环用 `std::set<Reader*> dropped` 去重；唤醒循环补上 `archReaders` |
| 3 | `reader/Reader.{h,cpp}` | `setBufferStartEnd` → **`resetWindow(start)`**：加锁，`bufferStart = bufferEnd = bufferScan = start` |
| 4 | `reader/Reader.cpp` | `resumeAt()` **不再置 `READ`、不再 notify**，只归还旧窗口 chunk + 复位；起读由 `parse()` 的 `setStatusRead()` 负责 |
| 5 | `parser/Parser.cpp` | 改用 `resetWindow(lwnConfirmedBlock*blockSize)`；主循环判据追加 `currentBlock*blockSize < getBufferEnd()` |
| 6 | `parser/Parser.cpp` | `50049` 在 `resumed && currentBlock == passStartBlock` 时**降级为 60058**（本趟作废 + 归档从头重读 + `resume_fallbacks++`）；只有"走到中途才遇到的坏头"仍致命 |
| 7 | `reader/Reader.cpp` | `read1/read2` 的 `10016` 改为 `REDO_CODE::STOPPED` + warning（本趟作废、下趟重来），不再 `10047` 停机 |
| 8 | `common/Ctx.{h,cpp}` | `readerSnapshot` → `std::array<ReaderSnapshot,16>` 按线程索引；`setReaderSnapshot(thread, …)`；60055 按 `watchdogThread` 取对应快照 |
| 9 | `replicator/ReplicatorOnline.cpp` | 删掉借 slot 1 的 `archReaderFor(1)`（归档 Reader 不再与在线/定位路径共用） |

---

## 4 与 20261030f 的差别

| 项 | `66443011`（20261030f） | `d785bfe7`（本版） |
|---|---|---|
| 归档 Reader 实例数 | **1**（四线程共享） | **每线程 1 个** |
| 关机释放 | 同对象删 2~5 次 → `10014` | 各删一次 |
| `60024 gate fileSize` | 会被别的线程清零 → 0 → 中段 STOPPED + 整档重读 | 不再被清（`5b24012` 的前提成立） |
| 切片续读首块 | 可能是**未读内存** → `50049`（致命）/`60058` | 窗口三位置一致，读不到未读内存 |
| 续读落点确实不对时 | `50049` → 停机 | `60058` → 重读该归档 |
| 读缓冲拿不到 chunk | `10047` → 停机 | `STOPPED` → 下趟重来 |
| 60055 的 `reader:` 段 | 全局一份，可能配错线程 | 按线程 |

---

## 5 验收判据

装置（podman RAC 复现台：4 线程交替 + `slice-pending-commits` 打开 + 归档造抖动）：

| 看什么 | 期望 | 不符合时说明什么 |
|---|---|---|
| `kill -USR1` 线程转储 | `-reader-arch-1..4` 四个不同名字 | ①没生效（仍在共享） |
| `60024 gate` 的 `fileSize` | 不再为 0 | ②/5b24012 的前提仍被破坏 |
| `60055` 的 `reader:` 段 | 文件名里的 `thread_N` 与 `(thread: N)` 一致 | ⑧没生效 |
| 切片续读 | 不再出现 `50049 … resumed: yes`；落点不对时应是 `60058` | ③④⑤没生效 |
| 正常/异常停机 | 不再 `10014 stacktrace` | ①没生效（或另有释放路径） |
| `10016` | 若出现应自愈（不 exit），且不刷屏 | ⑥没生效 / `read-buffer-max-mb` 偏小 |
| 回归 | `50053=0`、`60053=0`、`60046` 不增长、行数不丢 | — |

现场上线后 24 小时：进程存活、`10014=0`、`50049=0`、`60024 gate fileSize != 0`、
`delivered_per_s` 持续前进。

---

## 6 风险与未纳入（如实列出）

1. **读缓冲预算没有按线程切分**：`read-buffer-max-mb` 仍是全局预算，每个 reader 的窗口上限仍是整个预算。
   解析是单线程（任一时刻只有一个 reader 在读），空闲 reader 只留上一次的窗口（生产实测约 1 MB），
   因此暂不切分；若装置上 `10016` 反复，再降 `read-buffer-max-mb` 或加 per-reader 上限。
2. **P0-4（`delivered-scn` 全局丢弃 × drained release）未修**：现场 10 小时
   `restart_reread_drops=48,003,797`、启动 4 分钟就 99 万，与"只复读一次"的账对不上，
   需先用 `trace: TRANSACTION` 与 Doris 对账，再改按线程水位。
3. **`60055` 的 `watchdogProgressUt/Thread` 仍是全局**："no progress for: 0 s" 仍会出现（`watchdog-us` 配得偏小），
   只是 `reader:` 段现在归属正确。
4. **`60010` 仍未限流**：一次 60058 重读会在十几秒内产出两千多行。
5. **`slice-resume` 仍需装置压测**：本版把它的前提补齐了（独占 handle + 窗口一致），但
   "归档被删/换主"那条 20261030f 的 `stat` 校验仍只验大小，**同大小的换主仍是未关闭的窗口**。
6. 新增 4 个 reader 线程：`/proc/<pid>/status` 的 `Threads` 计数与 `kill -USR1` 转储都会变，
   监控若按线程名统计需要同步。

---

## 7 回滚

```bash
cp -p /opt/olr/OpenLogReplicator /opt/olr/OpenLogReplicator.bak-d785bfe7
cp -p /opt/dist/archive/OpenLogReplicator-66443011-20261030f /opt/olr/OpenLogReplicator
cd /opt/olr && printf '%s  OpenLogReplicator\n' 664430110044554df8e34fb45f07fdda31d9fbee4056c5ee200d73962c712e86 > SHA256SUMS.tmp
grep -v ' OpenLogReplicator$' SHA256SUMS >> SHA256SUMS.tmp 2>/dev/null; sort -u SHA256SUMS.tmp > SHA256SUMS; rm -f SHA256SUMS.tmp
sha256sum -c SHA256SUMS
service olr-zjsfrpt stop; service olr-zjsfrpt start
```

checkpoint 结构未变，双向兼容。

---

## 8 构建记录（构建机 172.17.75.130 / centos001 / el6）

```bash
# 源码 /opt/olr-build23，构建目录 /opt/olr-build23/build
cd /opt/olr-build23/build
/usr/bin/cmake3 .            # ← 必须重跑：否则 compiled 戳还是上一次 configure 的时刻
make -j2 OpenLogReplicator
```

| 门槛 | 结果 |
|---|---|
| 尺寸 / sha256 | 83,905,896 B / `d785bfe782664b6f94b3a707727299c2e5d5534a833efce8dbc84006cc03c1aa` |
| `--version` 第 2 行 | `compiled: 2026-11-02 05:56`（UTC，本地 13:56） |
| GLIBC 上限 | `GLIBC_2.12` |
| `NEEDED` 中的 rdkafka/zstd/prometheus | 0（静态链） |
| 新增告警 | 0（`Replicator.cpp:917`、`ReplicatorOnline.cpp:693/751/1809` 均为既有告警） |
| cfgcheck（`/root/olr-zjsfrpt.json`、`/root/olr-zjpvsrpt.json`） | `20003/30001/12001` 各 0 行，12 s 后强杀为预期 |

发布：`/opt/dist/OpenLogReplicator` + `.sha256`；旧件 `66443011` 与新件 `d785bfe7` 均归档在
`/opt/dist/archive/OpenLogReplicator-<sha8>-<日期>`。

---

## 9 装置验证（2026-11-02，podman 复现台 racstb1）

环境：`ORCLCDB` 单备库 + `slice-pending-commits: 64`、`arch-source: asm`、`start-scn 42547587`；
新件直接跑在容器里（`LD_LIBRARY_PATH=<ORACLE_HOME>/lib:/opt/el6rt`），走独立的 state 目录
`/var/lib/olr/checkpoint-g1`，不动装置原有状态。

| 判据 | 结果 |
|---|---|
| 启动 | 正常，`compiled: 2026-11-02 05:56`，全日志 **0 ERROR** |
| 解析/切片 | `processing redo` 46 趟；**切片 2 次 + `slice-resume` 2 次**，之后 `FINISHED 12689408 / 12720128` |
| P0-3（续读） | `50049=0`、`60058=0`、`60024 gate=0` —— 续读路径被真实走到，未再解出坏的 LWN 头 |
| P0-1（关机） | `SIGTERM` 后 **2 s 退出**，`10014=0`、`10017=0`、无 stacktrace |
| P0-2（隔离） | 关机线程转储出现 `thread: SRC-reader-arch-1`（新命名与工厂生效） |
| 回归 | `Kafka producer exit code: 0`、`writer hwm queue size: 1`、`60045` 正常（no-work / timeout 各一次） |

**限制（如实记录）**：该装置上 OLR 只起了 thread 1（`starting sequence for thread: 1 is: 20041`），
thread 2 的 45 份归档虽然 `resetlogs` 相同却没有出现在清单里。用装置自带二进制
（`OpenLogReplicator-r3`，即 20261030f 那条线）做 A/B，**同样只起 thread 1** ⇒ 不是本版引入的回归；
但"多个 reader 并存"这一形态在本装置上无法验证，需在生产（4 线程）或清单里能看到 thread 2 的装置上确认。

**附带发现**：容器里 `kill -USR1 <pid>` 不会产生线程转储 —— `Ctx::signalDump()` 开头有
`if (mainThread != pthread_self()) return;`，而信号由内核投给任意一个未屏蔽它的线程，多数不是主线程。
现场"转储恒为 0/0"除了 `acb5f8f` 修的那一半（发行版里计数被优化掉），还有这一半。建议去掉该判断
（转储只在 Ctx 锁下输出，不依赖调用线程）。
