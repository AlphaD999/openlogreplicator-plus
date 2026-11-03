# 2026-11-03a：修正 60055 的误报（thread 0 / 8891 s）

只换 `bin/OpenLogReplicator` 一枚。本版为 `20261102c`（`944345ec`）之上的一处修正，配置不改。

本版：84,039,120 B，sha256 `002deabff73c721b43b00e681eb72d6849813925d4accb08eba5a342784cf9a1`

---

## 1 问题

`20261102c` 把 60055 的进度改成按线程（`std::array<atomic,16>`）之后，2026-11-03 00:50 起现网每
**2.4 秒**一条：

```
WARN 60055 the parser made no progress for: 8891 s (thread: 0, file offset: 0, pending commits: 0)
          - locks held right now: deliveryMtx, delivery: in flight for 0 s, layers: sorter_pending=1481 ...
```

- `thread: 0`、`offset: 0`：**根本没有 redo thread 0**，这是没人写过的槽位；
- 时长从 `8891 s` 一路涨到 `34748 s`，而同一时刻解析/投递都在正常跑（`delivered_per_s=746`）；
- 一天约 **1.5 万条**，把日志真正有用的部分淹了。

根因：判据取"最久没动的槽位"，而**没人写过的槽位永远是 0**；`watchdogNoteIdle()` 又把**所有**槽位刷成
"刚动过"，于是 thread 0 成了永久最小值。上一版之前的单一全局时间戳没有这个问题（它被任何线程刷新），
所以这是我引入的回归。

## 2 改动

| 位置 | 改动 |
|---|---|
| `Ctx::watchdogActive[16]` | 新增。只有被 `watchdogNote()` 写过的线程才参与统计 |
| `Ctx::watchdogNoteIdle()` | 只刷新**活跃**槽位（等待归档不可读时不制造假活跃） |
| `Ctx::watchdogCheck()` | **判据拆成两个数**：`newest`（任一活跃线程最新时间）决定"是否真停了"；`oldest` 只用来**命名**，并用**它自己的** offset/pending/reader 快照 |

这样"某个线程本来就没归档可读"（安静实例、`60045` 释放）不会再被当成停摆，真停摆时仍然会报，而且报的是
真正卡住的那个线程。

## 3 验证

装置（podman racstb1，2 线程），打开 `OLR_WATCHDOG=1` 跑 26 s：

| 判据 | 结果 |
|---|---|
| `60055` 条数 | **0**（修复前同条件下每 2.4 s 一条） |
| 解析活动 | `processing redo` 227 趟、`result: FINISHED` 214 |
| `ERROR` / `10014` | 0 / 0 |

如实说明：**误报消除已验证**；"真停摆仍然报警"这条只做了代码审阅（装置上造不出真停摆）。

## 4 与本次事故的关系

本版只修诊断噪声，**不改投递/丢弃逻辑**，不解决 2026-11-03 的延迟（那是源端产出 ≈115-150 GB/h vs
OLR 实际推进 ≈28 GB/h，加上归档保留期只有 5 小时导致的追不上 + 归档被删）。
