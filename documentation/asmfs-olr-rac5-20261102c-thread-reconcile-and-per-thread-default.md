# 2026-11-02c：归档线程补齐（checkpoint 缺线程）+ `drop-delivered-per-thread` 默认改为开

本版换 `bin/OpenLogReplicator` 一枚；**配置一行不改**（开关有默认值）。
`20261102b`（`cee0a395`）**未部署**，被本版取代；`20261102a`（`d785bfe7`）的说明仍然有效。

本版：84,017,176 B，sha256 `97fc0d9d7bb1d50a8a92ce603fcc4ac09259ac9d1af052edc19b685dd364b057`

---

## 1 补齐：checkpoint 里缺的 redo 线程永远拿不到位点

`20261102b` 修了「起点 SCN 不覆盖某线程最老归档 → 该线程没有起始 sequence」。装置验证时发现**还有第二条路**
走到同一个结果，而且这条路是**升级必经路径**：

```
（用旧件跑，只解析 thread 1）
== 新件从这份 checkpoint 重启
restored from checkpoint thread 1: seq: 20090, offset: 0     ← 只有 thread 1
processing_thread2=0                                          ← thread 2 一份归档都没读
```

原因：`Replicator::run()` 里

```318:319:src/replicator/Replicator.cpp
                    if (metadata->firstDataScn == Scn::none() || metadata->sequence == Seq::none())
                        positionReader();
```

checkpoint 已经给出 `firstDataScn` 与 `sequence` ⇒ `positionReader()` **不执行** ⇒ `threadPos` 里没有
thread 2 ⇒ `archGetLogOnline()` 用「已知线程的最小 sequence」（thread 1 的 20090）当 SQL 下界 ⇒
thread 2 的 15320..15376 全被 `SEQUENCE# >= 20090` 滤掉，**永久不可见、无告警**。

**修法**：在 `archGetLogOnline()` 第一次取清单之前，用一条廉价查询给「有归档但没有位点」的线程补位：

```sql
SELECT THREAD#, MIN(SEQUENCE#) FROM SYS.V_$ARCHIVED_LOG
 WHERE SEQUENCE# > 0 AND RESETLOGS_ID = :i GROUP BY THREAD#
```

只对 `threadSequence(thread) == 0` 的线程 `setSeqFileOffset`，已有的位点不动；每次启动只跑一次
（`Replicator::archThreadsReconciled`）。日志会打一行：

```
redo thread: 2 has no saved position - starting at its oldest archived redo log, seq: 15320
（the checkpoint does not carry it）
```

**装置实测**：同一条命令 `processing_thread2` 从 **0 → 55** 趟，thread 2 的归档被读、被投递。

## 2 `drop-delivered-per-thread` 默认改为 1

### 2.1 为什么改：装置上做了确定性差分

步骤：用**旧件**（只解析 thread 1）跑 22 s，把全局 `delivered-scn` 推到 thread 2 整个 SCN 区间之上，
`kill -9` 留 checkpoint（`delivered-scn=42615217`，`threads` 只有 thread 1），快照；然后用新件从**同一份
快照**分别以 OFF / ON 跑 16 s：

| | OFF（全局水位） | ON（逐线程） |
|---|---|---|
| thread 2 解析 | 55 趟 | 55 趟 |
| **`dropping a commit` 条数** | **140** | **0** |
| `bytes_parsed_repeated` | 0 | 0 |
| `committed_reread_drops` | 0 | 0 |
| `60069` | 0 | 1（一次性，如实提示） |

`bytes_parsed_repeated = 0` 是关键：那一轮**什么都没被重读**，140 条不可能是"重读去重"。它们是
thread 2 **从未投递过**的提交，只因为 thread 1 早已把全局水位推高就被判成"已投递"丢掉了。
这就是现场 10-30 那 4,800 万条的形状（启动 4 分钟 99.7 万、`60045` 614 次释放对应 614 次
`reopened behind the low-water-mark`，而全天只有 1 次 `60058`）。

### 2.2 两种配置的失败方向

| | OFF（全局水位，0cd17ce 的原行为） | ON（逐线程 + 释放过就不丢） |
|---|---|---|
| 丢弃判据 | 所有线程共用一个最大 SCN | 只与提交自己线程的水位比 |
| 被 `60045` 释放过的线程 | **静默丢弃**（本次实测 140 条） | 永不丢弃（宁可重投） |
| 首次从**旧 checkpoint** 重启（无 `delivered-scn-by-thread`） | 去重正常 | 回退窗口**重投一次** + `60069` 提示 |
| 正常重启的 min-trans 回退窗口 | 去重正常 | 去重正常（逐线程更精确） |
| `60058` 重读同一归档 | 去重正常 | 去重正常（不置"释放"标记） |
| 失败表现 | **静默丢数**（不可见） | 重复（可见、可对账、有界） |

结论：ON 的失败方向是"多投一遍"，OFF 的失败方向是"少投且不报"——**丢数不能作为默认**，所以本版把默认
翻成 ON，开关保留（置 0 即回到旧行为，用于对账/回滚）。

### 2.3 代价（首次升级）

第一次用本版启动时，checkpoint 是旧件写的、没有 `delivered-scn-by-thread`，于是**这一轮不丢弃**：
`min-trans` 回退窗口会整段重投一次（量级 = 停机时未完成事务的回退范围；10-23 那次是 3.7 万提交）。
日志会打 `60069` 说明这件事；本版写出第一份 checkpoint 之后，逐线程水位就位，之后的重启恢复正常去重。

## 3 测试矩阵（全部在 podman 复现台 racstb1 上跑）

| 编号 | 内容 | 结果 |
|---|---|---|
| T1a | 起点 SCN 不覆盖 thread 2：旧件 vs 新件 | 旧：只 `thread 1: 20041`；新：`thread 1: 20041` + `thread 2: 15320` |
| T1b | 起点 SCN 覆盖两个线程（非回归） | 旧 `20042/15321`，新 **完全相同** `20042/15321` |
| T2 | 同一 checkpoint，OFF vs ON | 丢弃 **140 vs 0**（见 §2.1） |
| T3 | 配置不写开关（默认 ON）：生效口径 + checkpoint + 重启恢复 | `own redo thread` + `[{"thread":1,…},{"thread":2,…}]` + 按线程恢复，`60069=0`、`dropping=0`、`10014=0` |
| T4 | `20261102a` 的每线程 Reader / 关机 | 转储出现 `SRC-reader-arch-1` 与 `-2`；SIGTERM 2 s 退出，`10014=0` |
| T5 | 切片续读（`slice-pending-commits: 64`） | 2 次切片 + 2 次续读，`50049=0`、`60058=0` |

## 4 升级与回滚

**升级**（配置不改）：

```bash
cp -p /opt/olr/OpenLogReplicator /opt/olr/OpenLogReplicator.bak-e9269a16
cp -p bin/OpenLogReplicator /opt/olr/OpenLogReplicator && chmod 755 /opt/olr/OpenLogReplicator
cd /opt/olr && printf '%s  OpenLogReplicator\n' e9269a1648de8cb644b6717f50cd89782da13a56e2d69261bad09b45242cec1e > SHA256SUMS.tmp
grep -v ' OpenLogReplicator$' SHA256SUMS >> SHA256SUMS.tmp 2>/dev/null; sort -u SHA256SUMS.tmp > SHA256SUMS; rm -f SHA256SUMS.tmp
sha256sum -c SHA256SUMS
service olr-zjsfrpt stop; service olr-zjsfrpt start
```

升级后第一轮预期：`60069` 一次；`restart_reread_drops` 可能因为"回退窗口重投"而升高（**不是丢数**）。

**回滚**（默认 ON 之后 checkpoint 一定带 `delivered-scn-by-thread`，而旧件读它会抛 `20003`，所以要删键）：

```bash
service olr-zjsfrpt stop
# 1) 删掉最新 checkpoint 里的键（路径取 config 的 state.path）
f=$(ls -t /var/lib/olr/checkpoint/*.json | head -1); echo "patching $f"
sed -i 's/,"delivered-scn-by-thread":\[[^]]*\]//' "$f"
# 2) 换回旧件
cp -p /opt/olr/OpenLogReplicator.bak-66443011 /opt/olr/OpenLogReplicator
service olr-zjsfrpt start
```

只改"最新的那份"就够：加载的是 `*-chkpt-<scn>.json` 里 SCN 最大的那份（`ZJSF-chkpt.json` 指向它）。
若不确定，`grep -l delivered-scn-by-thread` 把带键的全部删掉即可。

## 5 构建

```bash
cd /opt/olr-build23/build && /usr/bin/cmake3 . && make -j2 OpenLogReplicator
```

| 门槛 | 结果 |
|---|---|
| 尺寸 / sha256 | 84,017,176 B / `97fc0d9d7bb1d50a8a92ce603fcc4ac09259ac9d1af052edc19b685dd364b057` |
| `--version` 第 2 行 | `compiled: 2026-11-02 07:32`(UTC, 本地 15:32) |
| GLIBC 上限 | `GLIBC_2.12` |
| NEEDED 中的 rdkafka/zstd/prometheus | 0 |
| 新增告警 | 0 |
| cfgcheck（两套现场配置） | `20003/30001/12001` = 0 |

发布：`/opt/dist/OpenLogReplicator` + `.sha256`；`d785bfe7-20261102a`、`cee0a395-20261102b`、
`e9269a16-20261102c` 都在 `/opt/dist/archive/`。

---

## 6 冒烟：清空 state + 从 CURRENT SCN 起步（现场流程）

现场是"历史数据不要、从 current scn 起步、先清 state 目录"。这套流程**已在本版验证通过**——
清不掉数据不是问题，问题是产品的丢弃判据在"从 start-scn 全量重放"时用错了（见 §2），现在按线程了。

脚本：`repro1014/smoke-current-scn.sh`（依赖同目录两个 .sql），在 racstb1 上跑完一轮约 60 s：

```
sh smoke-current-scn.sh <二进制> <基配置> [回退SCN数]
```

判据与结果（`PASS=11 FAIL=0`，回退 5000 SCN 以制造真实解析/投递工作量）：

| 编号 | 判据 | 结果 |
|---|---|---|
| A | `starting sequence for thread: N` 行数 == 有归档的线程数 | 2 == 2 |
| B | 运行期无 `dropping a commit` | 0 |
| C | 无 `60069` | 0 |
| D | checkpoint 带 `delivered-scn-by-thread`（逐线程值不同） | `[{1,42622638},{2,42621876}]` |
| E | `50049/50058/10014/10016/10047` 全 0 | 0 |
| E2 | 无 `ERROR` 行 | 0 |
| F | 不删 state 重启，按线程恢复 | 2 == 2 |
| G | 重启无 `60069` | 0 |
| H | 重启无 `10014` | 0 |
| J | 重启无丢弃 | 0 |
| I | 每线程一个 reader 实例 | `-reader-arch-1`、`-reader-arch-2` |

**注意两点（都不是缺陷）**：

1. **从"恰好 current SCN"起步时通常没有工作量**：线程会被定位到最后一个归档之后的 sequence，
   一段时间内没有新归档可读 → 不解析、不投递、**不写 checkpoint**、也不创建 reader 线程。
   此时只有 A/B/C/E 有意义；D/F/I 需要"有归档可解析"才有得看（脚本用回退 SCN 制造工作量）。
2. reader 是**懒创建**的：没有归档要读的那一轮，转储里不会出现 `-reader-arch-*`。

**本轮冒烟修掉的一个误报**：起初 `60069` 在"从未投递过任何东西"的运行上也会打（该运行既没有
`delivered-scn` 也没有逐线程数组，本来就没有可丢的东西）。现在只在"有全局水位、但缺逐线程数组"时提示，
即真正的"从旧件升级"场景。空闲源不再每次重启都喊一次。
