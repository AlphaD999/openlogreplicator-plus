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

## 6 `50080`：坏块转储（本版追加）

`50049`（invalid lwn scn）/ `50051`（did not find lwn at offset）在抛出前多一行 `ERROR 50080`，内容是**它当时
看到的那一块**：

```
ERROR 50080 bad block header - file: <path>, block: <n>, offset: <bytes>, blockSize: <n>,
     thread: <t>, seq: <seq>, reason: <text>, resumed: <yes|no>, passStartBlock: <n>,
     reader scn: <a> - <b>, header: vld=0x<hex> lwnNum=<n>/<n> lwnSize=<n> lwnScn=<scn>,
     file size: <n> (stat: <n>), raw[0..48]: <96 hex>
```

它要回答的是唯一重要的问题：**是归档坏了，还是我们读错了位置**。`offset` 可以直接喂给 `dd`（见 §7），
`raw[0..48]` 与手工读出的字节逐字节比对即可：**字节一致 → 归档/桥给的字节就是坏的**；
**手工读是好的头、日志里是垃圾 → 读路径（位置/窗口）错了**。

`resumed` 与 `passStartBlock` 说明这是不是"切片保存的位置"（那种情况由 `60058` 处理，不会停）。

### 一个意料之外的实测结论（对"中途坏块就停"的修正）

在装置上试了 5 种造坏块的方法（清零文件尾部、清零中段 4 MB、清零第 50 个带头标志的块、清零第一个记录头、
按 `lwnSize` 走链定位），**没有一次触发 50049/50051**：记录走查会跨过被清零的块（续块本身也带 LWN 标志），
而"落在错误的块上"多半是被**成员序号**检查抓住（旧的 50050），而那条在 `b2634ef` 里已经无条件降级成
`60058`。

也就是说：**"中途坏块导致停机"的暴露面比原来以为的小**——大部分走错位置的情况现在是 60058（本趟作废、
重读该归档），不是停机。真正还会停的只剩"落到的块连 LWN 标志都没有"和"头里的 SCN 越界"这两种，而 `50080`
现在会把它们当场拍下来。

## 7 手工核对坏块（`dd`）

用**日志里那条 `50080` 的 `file:` 路径**（映射后的挂载路径，不是 ASM 原名），以及它的 `block` / `offset`：

```bash
F='<日志里的 file: 路径>'
BLK=5184                     # 日志里的 block:
# 1) 原始字节（十六进制 + 可打印字符）
dd if="$F" bs=512 skip=$BLK count=1 2>/dev/null | od -A d -t x1z | head -12
# 2) 只解出关键字段（与 50080 的 header: 段对照）
dd if="$F" bs=512 skip=$BLK count=1 2>/dev/null | od -An -v -t u1 | tr -s ' \n' ' ' \
 | awk '{printf "vld=0x%02x lwnNum=%d/%d lwnSize=%d lwnScn=%d\n",
         $5, $25*256+$26, $27*256+$28, $29*16777216+$30*65536+$31*256+$32,
         $41*281474976710656+$42*1099511627776+$43*4294967296+$44*16777216+$45*65536+$46}'
# 3) 也可以用字节偏移（日志里的 offset:），块大小不必是 512
dd if="$F" iflag=skip_bytes,count_bytes skip=2654208 count=512 2>/dev/null | od -A d -t x1z | head -12
# 4) 文件大小与日志里的 file size / stat、以及 v$archived_log 的 blocks*block_size 对一下
stat -c '%s %n' "$F"
```

判定：`vld` 的 `0x04` 位表示"这一块声称是 LWN 头"；`lwnScn` 应落在 `50080` 的 `reader scn: a - b` 区间内。
两者都对、而 OLR 报坏 → 读路径；两者都不对 → 归档/桥。

