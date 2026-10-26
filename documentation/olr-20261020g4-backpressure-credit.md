# 2026-10-20g4：在途提交 credit、换出上限与停滞让出

本文说明 g4 三个新机制背后的**真实线程模型**、2026-10-23 那次积压的成因，以及每个旋钮的语义与回滚方式。面向现场运维与后续维护者。

## 一、线程模型（本文档纠正了早期描述）

| 角色 | 数量 | 说明 |
|---|---|---|
| 解析线程（replicator） | **1** | `class Parser final : public CommitSorter::Executor`（`Parser.h`），**不是 Thread**；`parser->parse()` 在 replicator 线程内同步调用（`Replicator.cpp`），由 `spawnThread(replicator)` 创建。4 个 **redo 线程是逻辑流**（RAC 的 4 个日志流），**串行解析**。 |
| Reader | **每个被读的归档 1 个** | `class Reader : public Thread`，`Reader::run()` 预读归档到共享读缓冲，与解析线程用条件变量交接。 |
| delivery | 1 | `DeliveryExecutor`，按 SCN 顺序构包并交给 writer。 |
| writer | 1 | `WriterKafka` / `WriterFile`。 |
| MemoryManager | 1 | 换出/换回（swap pump）。 |
| checkpoint | 1 | 每 `interval-s` / `interval-mb` 写 checkpoint 文件。 |

由此有两条结构性事实：

1. **一份慢归档会拖住所有 redo 线程**——解析只有一个执行者。这正是 g4.5（停滞让出）存在的原因。
2. **投递顺序由低水位决定**（各 redo 线程已解析 SCN 的最小值），与解析是否并行无关；慢线程照样让别人的提交投不出去。

## 二、2026-10-23 那次是什么形态

| 时间 | 解析速度 | `sorter_events` | `swap_entries` |
|---|---|---|---|
| 17:29–20:59 | 162–171 MB/s（2.3 GB 归档 13–16 秒） | 0 | 11–59 |
| 21:51 拐点 | — | 0 → **18,211**（thread 4 十秒内产出 1.8 万提交） | 53 → 945 |
| 22:00–23:30 | 3.8 → 1.6 MB/s（最慢一份 8,133 秒） | 峰值 **59,048** | 峰值 **18,245** |

链条：

```
一个 redo 线程十秒产出 1.8 万提交（批处理的行级提交）
        ↓ 它们高于低水位（等最慢的线程）
缓冲提交占满事务池 → 换出 pump 开始写盘（无上限）
        ↓ 读回与读归档抢同一批核与同一个挂载
解析逐块变慢（60055 最大只有 2 秒：是"慢"，不是"停"）
        ↓ 水位更推不动 → 更多缓冲 → 更多换出
```

关键：`60062`（chunk 背压）**一次都没触发**。因为事务一旦被换出，chunk 计数就回到 1，5.9 万个提交也碰不到"半个池"的上限。**换出把背压绕过去了**——这是 g4 要堵的第一个洞。

## 三、g4 的三个机制

### 1. 在途提交 credit（`source."pending-commits-per-thread"`）

- 语义：**一个 redo 线程在排序器里缓冲多少提交之后，它的解析在下一个记录头停下等投递**；
- 计量在 `enqueueCommit`（只累加计数），**等待在 `waitForBacklog`（记录头）**——记录内等待会等一个只有自己能发布的水位（实测 4,097 vs 4,096 的死锁），所以不放在记录内；
- 默认 **4096**；`0` = 自动（AIMD）；较大值 = 关闭（行为回到 g3），便于 A/B 与回滚；
- 超限量以"一条记录"为界（一条记录可能带几千个提交），文档与 60064 都写明；
- 新增告警 **60064**：同一线程被 credit 挡住超过上报阈值时说明"这不是内存问题，是水位被别的线程顶住"。

### 2. chunk 背压阈值收紧（半池 → 1/3 池）

`allowanceLocked` 的 `globalCap` 由 `chunks/2` 改为 `chunks/3`，更早减速；`minShareLocked` 公式不变，自动随 `globalCap` 缩放。

### 3. 换出上限（零参数，`swap_chunks` ≤ 池的 1/4）

- `Ctx::swappedChunks` 精确统计"当前在盘上的 chunk 数"，四条路径维护：落盘 +1（丢弃写入不计）、读回 −1、purge 丢弃 −(剩余)、release −(剩余)；
- 判据是 `swappedChunks >= getMemoryChunksMax()/4`，**O(1) 读计数器**，不遍历 `swapChunks`（故障态上万条目）；
- 到限后 `MemoryManager::getChunkToSwap` 不再选新事务，解析在**记录头**停下（不是卡在 `getMemoryChunk` 里变成 60043），交付读回后自动解除；
- 新增告警 **60065**（首次 + 每分钟一次）。

### 4. 停滞让出（原 g4.5，`source."stall-yield-us"`）

- 解析等数据发生在 `Reader::checkFinished` 内部的 `condParserSleeping.wait_for`；g4 给它一个截止时间，超时后返回"starving"；
- 解析器随即在**最后一个已验证的 LWN 边界**保存位置并返回，调度器把其它 redo 线程的归档先解析（复用已有的切片/重排队路径，`result: ... (sliced)`）；
- 恢复时从该边界续读，最多重解析"已开始的那条记录"的 redo（计入 `parse statistics` 的 `bytes_parsed_repeated`）；
- 默认 **5 秒**（5000000 µs），`0` = 关闭；仅当**队列里还有别的归档**时才让出，并且同一线程有 2 倍阈值的冷却；
- 新增告警 **60067**：同一线程一分钟内让出 4 次时提示"看那个线程的归档（是否还没写完/是否读得慢）"。

### 5. AIMD 自适应 credit（`pending-commits-per-thread` = 0 时）

- 拥塞信号：每线程被 credit 挡住的时间（`statCreditBlockedUs`），由解析侧累计、投递侧每 30 秒交换清零；
- 一个报告周期内被挡 ≥5 秒 ⇒ credit 减半（下限 512）；
- 没有线程被挡、且缓冲提交低于 credit×线程数/4 ⇒ 每次 +25%（上限 4096）；
- 每次调整打 **60066**（旧值 → 新值、原因、投递速率），当前值也在 `backlog layers` 的 `credit=` 里。

### 6. Event 池化

排序器事件改为有界空闲表（8192）复用，5 万级缓冲时省掉同等数量的 `new/delete`。

## 四、`backlog layers` 新增字段

```
... delivered_per_s=.. restart_reread_drops=.. swap_get_wait_ms=..
    credit=4096 pending_max=4098 swap_chunks=0 swap_chunks_max=3145728 swap_cap_hit=0 stall_yields=0
```

| 字段 | 含义 |
|---|---|
| `credit` | 当前生效的 credit（自动模式下是 AIMD 的值） |
| `pending_max` | 进程启动以来**单线程**缓冲提交数的峰值（应 ≈ credit + 一条记录；更大说明还有路径在绕开） |
| `in_delivery` | `sorter_pending` 之中已经交给 writer、但 `executeCommit()` 还没返回的事件数（g5）。writer 一堵这里就顶到 `sorter_pending`，用来区分"等水位"和"等 writer" |
| `swap_chunks` / `swap_chunks_max` | 当前在盘 chunk 数与上限（池的 1/4） |
| `swap_cap_hit` | 是否正卡在换出上限 |
| `stall_yields` | 累计"停滞让出"次数 |

## 五、验收口径

| 指标 | 目标 | 问题态（2026-10-23） |
|---|---|---|
| 每份 2.3 GB 归档解析 | **11–15 秒** | 1,039 / 2,433 / 8,133 秒 |
| `pending_commits` 稳态 | **< 1,000** | 38,000–59,000 |
| `swap_entries` 稳态 | **< 200** | 18,245 |
| `pending_max` | ≈ credit（4096）+ 一条记录 | — |
| `60043` / `60061` / `60062` | 不应新增 | — |

## 六、兼容与回滚

| 开关 | 关闭方式 | 关闭后行为 |
|---|---|---|
| credit | `"pending-commits-per-thread": 1000000` | 与 g3 一致（仅 chunk 背压） |
| 停滞让出 | `"stall-yield-us": 0` | 与 g3 一致（一份归档读到结束/被背压停） |
| 换出上限 | 无配置项 | 由池相对量决定，跟随 `memory.max-mb` |

checkpoint 文件格式未变；配置默认值即"新行为"，两项都可单独关闭做 A/B。

## 七、未覆盖的验证

- 本版在容器（GCC 8.5）全量编译通过；**el6 交付件尚未产出**（构建机不可达），三关门与 cfgcheck 待补；
- 装置上的三组 A/B（credit 关闭 / 4096 / 自动）与"归档不可读 → 让出"用例尚未执行（构建机与装置时间窗限制），上线前建议先跑；
- `Metadata.cpp` 的 `[STABILITY ANCHOR]` 私有回归仍需按清单执行。
