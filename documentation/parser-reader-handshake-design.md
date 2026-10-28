# Parser / Reader 握手协议与三条不变式

本文记录 2026-10-27 / 10-28 两次生产停摆的根因、修复与验证方法。涉及的代码在
`src/reader/Reader.cpp`、`src/replicator/Replicator.cpp`、`src/common/Ctx.{h,cpp}`。

## 1. 协议

一个 Reader 对象由 Parser 线程（Replicator 主循环）和 Reader 线程共同使用，靠 `Reader::status`
（`std::atomic<STATUS>`）交接：

| 阶段 | 谁置 `status` | 谁在等 | 等待条件 | 谁解掉 |
|---|---|---|---|---|
| `CHECK` | Parser（`checkRedoLog`） | Parser | `status != CHECK` | Reader：`mainLoop` 725 分支 `redoClose`/`redoOpen` |
| `UPDATE` | Parser（`updateRedoLog`） | Parser | `status != UPDATE` | Reader：741 分支 `reloadHeader` + 重置窗口 |
| `READ` | Parser（`setStatusRead`） | Reader | `status == READ` | Parser：消费并 `confirmReadData` 推进 `bufferStart` |
| 窗口满 | — | Reader | `bufferStart + bufferSizeMax <= bufferEnd` | Parser：消费 |
| `SLEEPING` | Reader | Reader | 被唤醒 | Parser 置 CHECK/UPDATE/READ |

Parser 是单线程（`Replicator` 线程同步调用 `parser->parse()`），所以"Parser 卡住"等于整条流水线停住。

## 2. 事故

2026-10-27 04:29 与 10-28 01:54，生产（ZJSF，4 实例 RAC）两次停摆，分别 21581 s 和 33000+ s，
Kafka 侧表现为断流，日志里只有一条 60055 在重复。

gdb 双栈（同一 Reader 对象）：

```
Reader 线程 : Reader::mainLoop        Reader.cpp:845   ← 窗口满等待（当时是嵌套 while）
Parser 线程 : Reader::checkRedoLog    Reader.cpp:1348  ← 等 status != CHECK
```

`checkRedoLog()` 已经置了 `status = CHECK`，但 841 行那个**嵌套在 READ 循环内部的 while** 只检查
`softShutdown` 和窗口是否满，**从不检查 `status`**；`bufferStart` 只能由 Parser 消费推进，而 Parser
正在等 `status`。环闭合。

触发条件（两者同时成立）：

1. 归档剩余长度 ≥ 读缓冲窗口（`memory."read-buffer-max-mb"`，生产 1024 MB）。生产两次的归档分别是
   1.43 GB 和 1.53 GB，切片点 465 MB / 524 MB，`buffer` 与 `limit` 恰好相等。
2. 解析中途切片，Parser 回到 `Replicator::processArchivedRedoLogs` 再次调用 `checkRedoLog`。

## 3. 三条不变式

- **A（断环）**：Reader 线程侧的任何等待，最多持续一个 `redoReadSleepUs` 就必须回到统一的状态评估点，
  重新检查全部前置条件（含 `status`）。禁止"只等单一条件的嵌套长循环"。
- **B（有界可恢复）**：Parser 侧任何握手等待都有总时长上限；超时先上报（60067），再超时则**主动复位**
  Reader 握手（置回 `SLEEPING` + notify 三个条件变量）并重试，绝不无限等待、不退出进程。
- **C（自解释）**：任何卡住都能从日志直接读出"谁在等谁"——60055 带 `reader:` 快照，60046 带正确文件名。

## 4. 实现要点

- `Reader::mainLoop` 的窗口满等待改为"单次 `wait_for`（谓词含 `status == READ`）+ 计数 + `continue`
  回读循环顶部"；顶部条件已含 `status == READ`，所以 CHECK/UPDATE 到来时必然退出内层循环。
- 窗口判定 `==` 改 `<=`：窗口被 `updateRedoLog`/`reloadHeader` 抬高后不得继续读。
- 60046 的节流计数改为成员 `bufferFullTicks`（窗口不满时清零）。
- `Reader::handshakeWait()`：累计等待 ≥ `handshake-timeout-us`（默认 30 s，0 = 关闭）报 60067，
  ≥ 4 倍时强制复位；连续 2 次复位无效才 `ret = STOPPED; return false`，交回既有 retry / 10009 / 10074。
  每次请求开始时重置计时（否则第二次请求会继承上一次的年龄）。
- `Ctx::setReaderSnapshot()` + `ReaderSnapshot`：Reader 在每次状态变化与每个窗口满 tick 推送快照
  （status / buffer / limit / size / 等待时长 / 文件名），60055 在独立小锁下读取。锁序：
  `Reader::mtx` → `snapshotMtx`，单向。
- `fileName` 增加加锁副本 `fileNameSafe`（`setFileName` / `fileNameForDiagnostics`）：原来 Parser 赋值、
  Reader 打印是 `std::string` 的无锁并发读写，导致 60046 打印的是**上一个归档**的名字。

## 5. 复现方法（装置）

三个条件缺一不可，否则大概率复现不出来：

1. **归档的 redo header 声明大小必须等于文件实际大小**。`asmcmd cp` 出来的一些归档 header 只声明几 MB，
   OLR 以 header 为准读到几 MB 就当 EOF，窗口永远填不满（60046 一条都不出）。
2. `redo-read-sleep-us` 要足够小（如 1000）：Reader 必须跑在 Parser 前面才能填满窗口。
3. Parser 要晚于 Reader 到位：用测试钩子 `source."debug-handshake-hold-us"`（`Replicator.cpp:1081` 前
   sleep，默认 0）人为制造这个时序；生产里它是"Parser 16 MB/s vs Reader 70 MB/s"的自然结果。

命中判据：`60046` 出现且 `bufferEnd - bufferStart == limit`、**只有 reader 侧文案**；
`gdb -p <pid> -batch -ex "thread apply all bt"` 同时看到 `Reader.cpp:845` 与 `Reader.cpp:1348`。

修复后同一配置：切片继续、`60046` 的等待秒数被下一次切片打断（0~4 s），`60067` 为 0。

## 6. 新增/变更的告警

| 码 | 含义 |
|---|---|
| 60067 | 握手未被服务（含 status/buffer/limit/size/file/reader 线程状态），以及"强制复位" |
| 60055 | 末尾追加 `reader:` 快照与 `waiting=Ns` |
| 60046 | 文件名改为加锁副本；重复出现且只有 reader 侧文案 = 握手互锁 |

## 7. 已知遗留

- `fileName` 的裸成员仍在 Reader 内部的非诊断路径上使用（redoOpen 等），与 Parser 的赋值仍有理论竞争；
  诊断路径已走加锁副本。
- L4（切片恢复不重开归档）未做：它能把整条触发链去掉（省一次 `redoClose`/`redoOpen`），需要单独压测。
