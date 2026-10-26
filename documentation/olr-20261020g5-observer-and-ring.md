# 2026-10-20g5：排序器之外的缓冲看得见、也被限住

> 上一版（g4，`ea6334d`）解决"一个 redo 线程能跑多快"：在途提交 credit（60064）、chunk 阈值 1/3 池、
> 换出上限（60065）、停滞让出（60067）、AIMD（60066）。这一版解决它**看不见的另一半**：排序器之后的
> 三段缓冲（builder 输出环 / writer 队列 / librdkafka 队列），以及一个让 60064 永远打不出来的阈值缺陷。

## 一、四个新字段（`backlog layers`）

```
backlog layers: sorter_events=… sorter_scns=… sorter_pending=… in_delivery=… writer_queue=… writer_queue_hwm=… builder_out=… pending_by_thread=[…] …
```

| 字段 | 含义 | 期望 |
|---|---|---|
| `in_delivery` | `sorter_pending` 中已被投递线程取走、`executeCommit()` 尚未返回的事件数（g5 把计数延到交付完成） | 稳态 0~几十；接近 `sorter_pending` ⇒ 卡在 writer/客户端 |
| `writer_queue` / `writer_queue_hwm` | writer 尚未被客户端确认的消息数及其峰值 | 正常 0~几十；钉住不降 ⇒ 客户端不接 |
| `builder_out` | builder 输出环里的 chunk 数（已构包、等确认） | 远小于上限；等于上限 ⇒ **60068**，交付被硬停在这里 |

## 二、两处机制改动

### 1. builder 输出环设上限（零参数，`60068`）

环按 64 KB 池块链式增长，此前只受 `memory.max-mb` 约束：客户端不读时能把整个池吃掉，而
`sorter_pending` 一直是 0（装置实测：5 万提交、`sorter_pending=0`、`writer_queue` 钉在 21、
消息全在环里）。现在环的上限是**池的 1/8**，到限后交付线程在 `Builder::builderRotate()` 等待，
每 30 秒最多报一次 **60068**。

可用 `memory."builder-out-chunks"` 显式指定（`0` = 池的 1/8，默认；`1..1000000` = chunk 数）。
启动打印：

```
memory: builder output ring: 24576 chunk(s) of 64 KB (a quarter of "max-mb"; "builder-out-chunks" = 0)
```

### 2. 60064 的判定阈值修正（这是一个缺陷）

```
BACKPRESSURE_REPORT_US    = 30 s   ← 60064 原来的判定阈值
BACKPRESSURE_EMERGENCY_US =  5 s   ← 到了就让投递"无视水位强投"
```

解析被 credit 挡住后，5 秒一到就应急放行，等待结束、计时归零 ⇒ 单次等待永远到不了 30 秒 ⇒
**60064 一次也打不出来**。这解释了 2026-10-23 现场为什么 `60062`/`60064` 全都没有：不是没堵住，
是告警阈值比逃生阈值还大。现在 60064 用独立阈值 **3 秒**，并且**每线程每分钟最多一条**。

## 三、运维与诊断

| 项 | 说明 |
|---|---|
| **60031 限流** | 原来 writer 每次重试都打两行（1 ms 一次 ⇒ 约 1,200 行/秒）。现在计数 + **每分钟一条**，报文含失败次数、最后错误，并区分"队列满（在等）"与"消息被丢弃" |
| **线程转储** | `kill -USR1 <pid>` 的状态以前在发行版里恒为 `0/0`（`contextSet` 被 `-DTHREAD_INFO` 包住）。现在**所有构建都记录**，且打印名称：`delivery context: sleep reason: BUILDER_RING_FULL`、`Replicator context: tran reason: none` |
| **`source."debug-hold-scn"`** | **测试钩子，生产禁止使用**：把低水位钉在指定 SCN，用于复现"一个 redo 线程爬得极慢"的形态并验证 credit。启动时以 WARN 明确喊出，`0` = 关（默认） |

## 四、装置实测（2026-10-26）

| 场景 | 配置 | 结果 |
|---|---|---|
| 客户端不接 | `builder-out-chunks: 2` + `linger.ms: 1000`（broker 健康） | `builder_out` 恒为 **2**、`60068` 6 次、`60031` **0** 次、`writer_queue_hwm=29` |
| 水位被顶住 | `debug-hold-scn: 1`、credit 4096、5,000 行级提交 | **`pending_max = 4316`**（= credit 4096 + 一条记录）、**`60064` 触发**、`60062=0`、`builder ring 0 of 24576` |

60064 原文（可自证是哪一类问题）：

```
the parser is stopped by its commit credit for: 3 s - redo thread: 1 holds 4316 buffered commit(s)
against a credit of 4096, and the swap bound is not the reason; deliverable now: 0 of 4316 buffered
event(s), of which 0 are in the delivery (the rest waits for the water-mark),
builder ring: 0 of 24576 chunk(s), thread watermarks: 1: 38788246
- this is not a memory problem: ...
```

**结论：两条故障路径各有明确归属**——客户端不接 ⇒ 环满 ⇒ `60068` + `builder_out` 到顶；
水位被顶住 ⇒ 排序器停在 credit ⇒ `60064` + `pending_max ≈ credit + 一条记录`。

## 五、cfgcheck（el6 上实跑）

| 用例 | 结果 |
|---|---|
| `olr-zjsfrpt.json` 原样 | 0 错误；`builder output ring: 24576 chunk(s)`（12288 MB / 64 KB / 8）、`commit credit: 4096 (default)` |
| `olr-zjpvsrpt.json` 原样 | 0 错误；`16384 chunk(s)` |
| `builder-out-chunks: 2` | 0 错误；打印 `2 chunk(s) of 64 KB ("builder-out-chunks")` |
| `builder-out-chunks: "bogus"` | **20003**（不是无符号 64 位整数） |
| `builder-out-chunks: 2000000` | **30001**（0 或 1..1000000） |
| `pending-commits-per-thread: 0` + `debug-hold-scn: 1` | 0 错误；打印 `commit credit: automatic (4096 down to 512 and back, AIMD)` 与 `TEST HOOK …` |

## 六、兼容与回滚

- **现场配置不需要改**：`builder-out-chunks` 默认 `0`（池的 1/8）、`debug-hold-scn` 默认 `0`（关）、
  `pending-commits-per-thread` 默认 4096、`stall-yield-us` 默认 5 s。
- 回滚到 g4 行为：只需 `"builder-out-chunks": 0`（默认即如此）+ `"pending-commits-per-thread": 1000000`
  + `"stall-yield-us": 0`；彻底回 g3 则换回 `615d2069…` 那一枚件。
- **`debug-hold-scn` 绝不能出现在生产配置里**：它会让所有提交在内存里等（只用于验证 credit）。
