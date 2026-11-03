# 2026-11-03b：把"清单有、读不到"和"投递停了"变成日志里的一句话

只换 `bin/OpenLogReplicator` 一枚，配置可选加一个键（不加也有默认值）。

本版：84,094,952 B，sha256 `04ebf675500e531b27ee938cc4759664625d679f31abb7fbd84de0b00f8e4cbb`
（`compiled 2026-11-03 05:23` UTC）

---

## 1 背景：2026-11-03 那次 5 小时停滞，日志里一句话都没有

现场形态（`olr-zjsfrpt 27.log`）：

* 归档被 5 小时保留期删掉了，但**归档清单里还在**；
* 于是 OLR 每秒重试一次打不开的归档：**每个文件 61 条 `10003` + 61 条 INFO**（`~3000 行/小时`）；
* `processing redo` 从 09:10 起一条都没有，`delivered_per_s=0`，`delivery_lag_ms` 单调上升；
* **日志里没有任何一行**说"这个归档列出但读不到已经 N 分钟"或"投递已经 N 分钟没动"。

结果是人靠盯 Prometheus 才发现的，发现时已经过去 5 小时。本版补的就是这两句话。

## 2 `10003` → `60072`：清单里有的文件读不到

| 位置 | 改动 |
|---|---|
| `Ctx::archUnreadable()` | 每个文件**每分钟最多一行**；跨过"半个重试预算"（`archReadTries/2`，现场 60 → 第 30 次）升级为 **`60072`**，只升级一次，之后每分钟最多一行 |
| `ReaderFilesystem::redoOpen()` | 原来每次 `stat()` 失败都 `error(10003)`，改走 `archUnreadable()` |
| `Replicator` 的重试循环 | **删掉每次重试的 INFO 行**（"is not ready for read, sleeping …"），间隔已经写进 `10003`/`60072` 的报文里 |
| `Ctx::archReadable()` | 文件恢复可读时打一行 `readable again after: N s (attempts: M)`——"来晚了"是正常的，"永远不来"才不是 |

`60072` 的报文（现场可直接照做）：

```
WARN 60072 file: <path> - the archive list names it but it has not been readable for: 45 s
     (attempts: 30, last error: get metadata returned: No such file or directory)
     - every redo thread that waits for it is stopped; check the archive mount
     (and the retention of the archive directory, an archive can be deleted while it is still in the list)
```

### 实测（装置，把 `path-mapping` 指到不存在的目录来等价复现）

| | 旧件 `944345ec` | 新件 `04ebf675` |
|---|---|---|
| 同一份归档、同样 18 s | **`10003` × 10**（每次重试一行） | **`10003` × 1**（第 1 次） |
| 跑到第 30 次（~45 s） | —— | **`60072` × 1** |

也就是：一个文件从 60 行降到 2 行，而且第 2 行是**结论**，不是噪声。

## 3 `60071`：投递停了（lag 上升的那件事）

新增 `state."delivery-stall-us"`（默认 **300000000** µs = 300 s，**0 表示关闭**）。

判据（由 checkpoint 线程每 0.1 s 看一次，**不依赖 `watchdog-us`**）：

* 这一轮至少投递过一条提交（否则"从 current scn 起步/源很安静"会被误报）；
* 距上一次交给 writer 已经超过阈值；
* 管道里确实还有活：`sorter_pending + in_delivery + writer_queue + builder_out > 0`。

报文：

```
WARN 60071 the delivery handed nothing to the writer for: 4218 s while 2048 item(s) are waiting
     (sorter_pending=2048, in_delivery=0, writer_queue=0, builder_out=16), last delivered scn: <scn>,
     commits handed over in this run: <n> - this is what a rising delivery_lag_ms looks like from the inside;
     'delivery-stall-us' sets the threshold (0 disables it), layers: sorter_events=… 
```

退避与 `60055` 一致：阈值、2×、4×…最多 8×，所以真停摆也不会刷屏。

### 实测（装置，把阈值临时设成 500 ms 以在 45 s 内看到）

```
WARN 60071 the delivery handed nothing to the writer for: 593 ms while 2 item(s) are waiting
     (sorter_pending=0, in_delivery=0, writer_queue=0, builder_out=2), last delivered scn: 42902739, …
```

45 s 内 4 条（0.5 s → 1 s → 2 s → 4 s 退避），阈值 300 s 时正常运行不会出现。

## 4 上线/回滚

不变（清 state）：`state."delivery-stall-us"` 是**可选**键，不写就用默认 300 s；写 0 关闭。
不需要动 checkpoint 格式，也不需要迁移任何东西。

## 5 回归

* 装置冒烟 11 项（清 state + 从 current 起步、回退 SCN 制造工作量）：见交付记录；
* `60072`/`60071` 都只在各自的条件成立时出现，正常运行时 0 条。
