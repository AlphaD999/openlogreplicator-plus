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

> **实施记录(2026-09-11) — D1 补充:归档留在 ASM 的旁路方案**:
> 现场归档量约 900 GB/h,增加文件系统归档目的地(`log_archive_dest_2`)会产生等量的额外写入,不可接受。
> 因此新增**外部 asmfs + 本地 FUSE** 方案:asmfs(第三方只读 FUSE)把 ASM 暴露成文件树,OpenLogReplicator 与 asmfs **同机部署于 RAC 宿主机**,直接用 `path-mapping` 把 `+` 路径改写到 FUSE 挂载点,不经过网络。
> 这**不违反 D1** —— D1 排除的是"自研原生 ASM Reader",本方案 OLR 侧不引入任何 ASM 解析代码,只多一条归档路径来源判定。
>
> 改动清单(6 个文件):
> - `src/common/Ctx.h`:新增 `bool asmArchives{false}`(**未占用 `flags` 位域**,`REDO_FLAGS` bit0~18 已满);
> - `src/replicator/ReplicatorOnline.h`:新增 `SQL_GET_ARCHIVE_LOG_LIST_ASM`(`AND NAME LIKE '+%'`)与 `SQL_CHECK_ASM_ARCHIVED_LOG`;`SQL_CHECK_FS_ARCHIVED_LOG` 加 `FETCH FIRST 1 ROWS ONLY`(避免大归档量库上全表 `COUNT(*)`);
> - `src/replicator/ReplicatorOnline.cpp`:启动日志新增 asm 分支;RAC 检测的检查 SQL 与 hint 按模式二选一;取数循环切换 SQL 并**按 `(thread, sequence)` 去重**(多 ASM 目的地时,队列只用 `(thread, sequence)` 做键,同键行出队顺序不确定);
> - `src/OpenLogReplicator.cpp`:`readerNames` 白名单加 `"arch-source"`;新增取值解析与校验(`filesystem` | `asm`);
> - `documentation/json/2.reader.adoc`、`documentation/log-messages/1.runtume-error-messages.adoc`:参数与 10073 双分支文案;
> - `scripts/grants.sql`:补 `GRANT SELECT ON SYS.V_$THREAD`(原先漏了,而 `loadDatabaseMetadata` 的启动授权检查 `checkTableForGrants` 已把它列为必需表,不补会导致启动即失败)。
>
> 新增配置:`reader."arch-source"`,`"filesystem"`(默认,行为与改动前完全一致) | `"asm"`。
>
> 已知外部依赖约束(来自对 asmfs 2.1.3 的源码审计):asmfs 默认改写归档日志首块 magic —— 这是**正确行为**(对照 `asmcmd cp` 验证过,见 `asmfs-2.1.3/doc/magic.md`),19c 归档日志需要它才能被当作普通文件解析,因此**不要**加 `--no-magic`;存在 `expect/unwrap` panic 点;元数据操作串行在一把 `Mutex<OracleConnection>` 上;ASM 重启导致 group number 变化后需重挂两端。
>
> 补充:`src/oraenv.rs` 的 `bootstrap_oracle_env()` 会扫 `/proc` 找 `asm_pmon_+ASM`、从其 `/proc/<pid>/exe` 反推 `ORACLE_HOME` 后 **re-exec 自身**。因此 —— **在 RAC 容器内(有 ASM pmon)可自举成功**;只有独立 sidecar 容器(无 ASM pmon)才需手工 `ORACLE_HOME`/`LD_LIBRARY_PATH` + `--conn`。这条把"容器内无法自举"的结论范围收窄了,并成为"附录 C 把 asmfs+OLR 编进 RAC 镜像自验证"可行的关键依据。
>
> 更正(2026-09-14):上文"**不要**加 `--no-magic`"的结论已被受控 A/B 实验推翻 —— 其余条件全同、OLR 已在轮询时制造新归档,不加 `--no-magic` 需 56 次 `ERROR 10005` 才自愈,加了则 0 次。asmfs 侧文档 v1.5 已按此更正。

> **实施记录(2026-09-14) — bug#3 定位与修复:启动即卡死(静默挂起)**
> 现象:RAC + 归档 asm 源,全新启动(start-scn 位于归档中部)处理完**第一个**归档后再无任何输出,
> CPU 近 0、checkpoint 不写、进程既不退出也不报错(实测静默挂死 1 小时以上)。
> 定位(gdb 线程栈 + 运行时内存计数器,非推断):
> - Reader 卡在 `Ctx::getMemoryChunk(READER, swap=false)`,Parser 卡在
>   `TransactionBuffer::addTransactionChunk → Ctx::swappedMemoryGrow → getMemoryChunk(TRANSACTIONS)`;
> - 计数器:`memoryChunksAllocated=4096=memoryChunksMax`、`free=7`(恰等于保留区),
>   `swapChunks.size()=4070`、`swappedMB=0`、`commitedXids=0`;
> - 事务 trace 实测 `begin=489 / commit=488 / commit-not-matched=16`,而 Manager 报告
>   "held by 489 open transactions" —— **已提交事务一个都没释放**。
> 根因:`Parser::processRedoLog` 里唯一的事件冲刷调用 `commitSorter->flushReady()` 位于
> `if (lwnScn > metadata->firstDataScn)` 分支内。全新启动时 Reader 从**归档开头**解析
> (SCN 3667027),而 `firstDataScn` 是请求的 start-scn(4218803),这 130 MB 区间内
> `flushReady` 一次都不调用 → 已提交事务全留在排序器里,每个占 1 个 1 MB chunk
> (单 chunk 事务**不可换出**,`MemoryManager::getChunkToSwap` 跳过 `chunks.size() <= 1`)
> → 内存池耗尽 → Parser 与 Reader 互等 → 死锁。生产(3 GB/5 min)量级下这会直接阻断上线。
> 修复(2 处):
> - `src/parser/Parser.cpp`:`lwnScn <= firstDataScn` 分支补 `flushReady()`(冲刷仍受低水位门控,
>   输出顺序不变);
> - `src/common/Ctx.cpp`/`MemoryManager.cpp`:OOM 升级判定改为"**Parser 线程**被阻塞"
>   (原先只在 `MEMORY::PARSER` 模块分配失败时置位,漏掉了事务 chunk 增长),
>   池耗尽且无可换出时按现状输出 60043 + 定位 hint(含 open/swappable 事务数、各模块占用),
>   随后 `wontSwap` 抛 10017 退出 —— 静默挂起改为**明确报错**。
>   顺带修正 `memoryModules[]` 名字数组(缺 `"misc"` 导致模块名整体错位、writer 取到空串)。
> 验证(容器内 RAC,同一环境同一场景):同一配置下从"处理 1 个归档即挂死"变为
> 连续处理 5 个归档(thread1 seq109→112、thread2 seq45→48),checkpoint 正常落盘,
> 无 60043/10017;并由 checkpoint 续跑成功;此前因该 bug 一直未能进入 Kafka 的
> 70001-70003(`OLR_USER.CDC_TEST`,long_txn_a/b/c)在修复后写入 Kafka。
> 压测定容结论:`max-mb` 至少覆盖"峰值并发**未提交**事务数 × 1 MB" + 读写缓冲
> (与"最大单事务 ×110%"是两条独立约束,前者在实测中先于后者触发)。

> **实施记录(2026-09-14) — 双连接:字典走主库、归档清单走备库**
> 现场形态:主库 3 GB/5 min 切换,归档经 arch async 传到备库,备库 ASM 由 asmfs 暴露为本地文件树。
> 实测(生产环境样本)主备同一 `(thread, sequence)` 的归档文件名**不同**:
> 主库 `+ZJSF_ARCH/zjsf580/.../thread_1_seq_1134956.6179.1243972045`、
> 备库 `+ZJSF_ARCH/zjsfstb/.../thread_1_seq_1134956.2277.1243972047` —— 目录段是各自的
> `db_unique_name`,尾段是各自 ASM 生成的 incarnation;而 `applyMapping`(`Replicator.cpp:431-445`)
> 只做**前缀替换**,主库的 `NAME` 无法翻译成备库路径。同时 SCN 区间主备一致(同一份 redo),
> 所以正确的分工是:**字典/元数据在主库查(可写、可闪回,顺带绕开 ADG 闪回限制),文件清单在备库查**。
>
> 改动(4 个文件):
> - `src/replicator/ReplicatorOnline.h`/`.cpp`:新增可选 `DatabaseConnection* connArchives` +
>   `getArchivesConnection()`(未配置时返回 `conn`,零行为差异);构造函数增加
>   `newArchivesUser`/`newArchivesPassword`/`newArchivesConnectString`;`checkConnection()` 拆出
>   `checkConnectionFor(conn)` 并同时校验两条连接;`goStandby()` 一并断开;`checkTableForGrants()`
>   增加连接参数。**路由到备库的只有 3 处**:`SQL_GET_ARCHIVE_LOG_LIST[_ASM]`、
>   启动可达性检查 `SQL_CHECK_(ASM|FS)_ARCHIVED_LOG`、`SYS.V_$ARCHIVED_LOG` 的授权检查;
>   其余(字典 `AS OF SCN`、`V_$DATABASE`/`V_$THREAD`、SCN→sequence 定位、时间→SCN、incarnation)仍在
>   `conn` 即主库。
> - `src/OpenLogReplicator.cpp`:reader 白名单与解析新增 `archives-server`/`archives-user`/`archives-password`;
>   `user`/`password` 作为默认值;只给 `archives-user`/`archives-password` 而无 `archives-server` 直接
>   报 30001。
> - `documentation/json/2.reader.adoc`:三个参数说明(含"清单即到货闸门"的行为说明)。
>
> 设计要点:
> - 归档清单在备库查,天然是**到货闸门** —— 只有已接收并注册的归档才出现在清单里,避免了"主库已有、
>   备库未到"时去读不存在文件的时序问题(实测传输延迟约 2 s,但慢网络下可能超过读取重试窗口);
> - `metadata->resetlogs`(绑定到清单 SQL 的 `RESETLOGS_ID`)来自主库 `V_$DATABASE`,备库接收的归档
>   行携带同一 resetlogs id,过滤条件一致;角色切换导致 resetlogs 变化时仍需按既有语义重启;
> - 启动时备库若尚未追平 `start-scn`,定位(`SCN→sequence`)在主库完成,再由读取重试等待文件到达。
>
> 验证(容器内 RAC,两个实例模拟主/备):
> - `archives-server` 指向 node2 后,启动日志出现 `archived redo log list will be read from: ...`,
>   两条连接分别建立,归档照常消费;
> - `arch: "online-keep"` 下在 node1 与 node2 **各观测到一个 OLR 会话**(`v$session` 中
>   `program like '%OpenLog%'`),证明第二条连接真实存在且用于清单;
> - 负向用例:`archives-user` 而无 `archives-server` → 启动即 `ERROR 30001`,不会静默回退。
>
> 待现场验收(需真实物理备库):主备 `db_unique_name`/incarnation 不同时的**端到端**取文件验证,
> 以及 `path-mapping` 指向备库目录(`+ZJSF_ARCH/zjsfstb` → FUSE 挂载点)的配置核对。

> **实施记录(2026-09-14) — 第 3 步:吞吐实测与定容模型(含一个必须先修的容量缺陷)**
> 测试形态:容器内 RAC(12 vCPU / 48 GiB,OLR 与 DB 同机),造 300 万行数据(20 个已提交事务)+
> 双实例切日志,产出 thread1 seq113/114/115 = 63 / 206 / 144 MB,thread2 seq49 = 26 MB。
>
> **一、解析吞吐实测**(16 GiB 池,日志逐归档时间戳)
> | 归档 | 大小 | 处理耗时 | 速率 |
> |---|---|---|---|
> | seq 113 | 60 MiB | 2 s | ~30 MB/s |
> | seq 114 | 196 MiB | 2 s | ~98 MB/s |
> | seq 115 | 137 MiB | 1 s | ~137 MB/s |
> 取保守值 **≥30 MB/s**,大归档接近 100 MB/s+。生产需求 3 GB/5 min = **10 MB/s**,
> 即解析侧有 **3~14 倍余量**;瓶颈不在解析速率,而在下面这条容量约束。
>
> **二、容量模型(实测修正)**:`swap-path` 目录**全程 0 字节**——本次没有发生任何换出;
> 进程 RSS 只有 1.15 GB,却吃掉了 4067 个 chunk。原因是 chunk 为 `aligned_alloc(1 MB)`,
> 只有被写的 4 KB 页计入 RSS,所以 **RSS 严重低估占用**;真实占用必须看 chunk 计数:
> - 4 GiB 池(`max-mb: 4096`):解析 63 MB 归档时,池被 **4067 个"打开事务"**占满
>   (每个 ≥1 个 1 MB chunk,与实际数据量无关)→ `10017 out of memory`;
> - 16 GiB 池(`max-mb: 16384`):同一批归档全部通过,0 次 60043/10017,~6 s 消化 440 MB。
>
> **三、根因:`CommitSorter` 低水位 × 归档级调度**
> `Replicator::archiveRedoQueue` 按 `(thread, sequence)` 排序(`parserCompare`),**没有跨线程交错**:
> 先解析完 thread1 的整个归档,再解析 thread2 的。低水位 = 各线程已解析 SCN 的最小值,
> 于是解析 thread1 归档的**全过程**里,该归档中所有已提交事务都因水位未过而滞留内存
> (实测该归档横跨约 2 小时 SCN 区间,内含 4000+ 个后台小事务,每个占 1 MB)。
> 换句话说:**峰值内存 ≈ 领先线程单个归档内的事务个数 × 1 MB**(与小事务/大事务无关)。
> 生产 3 GB 归档若含数万事务,即需数十 GB —— 这是上线前必须先处理的缺陷。
> 注意:`swap-mb` 对此**无效**(它只能换出"多 chunk 的大事务",单 chunk 事务按设计不可换出)。
>
> **四、修法(建议按序)**:
> 1. **跨线程交错解析**(根治):按 SCN/偏移切片轮转解析两个线程(例如每 8~16 MB 或每个 LWN 边界切换),
>    低水位间隙从"一个归档"降到"一个切片",滞留事务从数千降到数十。可行性依据:checkpoint 已按
>    线程持久化 `(sequence, offset)`,归档内任意偏移续读已被验证可行(实测 `offset: 12578816` 续跑成功);
> 2. **放宽单 chunk 事务的换出限制**(缓解):允许 `chunks.size() == 1` 参与换出,可把容量从
>    `max-mb` 提到 `max-mb + swap-mb`(约 2 倍),但仍是线性受限,只能作为过渡;
> 3. 短期上线兜底:`max-mb` 按"单个归档事务数 × 1 MB × 1.5"配置,并在监控上盯 60043/10017(现已可见)。
>
> **五、定容公式(现场填写)**
> ```
> max-mb  ≥ 1 MB × (一个归档窗口内的最大事务数) × 1.5 + read-buffer-max-mb + 64
> swap-mb ≥ 1.5 × (最大单事务 MB)      # 只对多 chunk 大事务有效
> transaction-max-mb: 若存在超大事务(> 池的 1/3)则设为阈值跳过,避免单事务吃满
> arch-read-tries × arch-read-sleep-us ≥ 归档传输最坏耗时(生产实测 ~2 s,建议 ≥ 60 s)
> ```
> 现场需先统计:每 5 分钟窗口的**事务个数**与**最大单事务大小**(可用 AWR/`V$TRANSACTION`
> 采样或按业务模型估算),这是 `max-mb` 的第一约束(优先级高于"最大单事务×110%")。

> **实施记录(2026-09-14) — 第 3 步续:跨线程切片轮转 + Reader 文件大小修正**
> 目标:消除上一条的容量缺陷 —— 让领先线程不必等落后线程追上就能继续,同时把"低水位间隙"
> 从"一个归档"压缩到"一个切片",使滞留事务数(每个 1 MB chunk)与内存占用有界。
>
> 改动(6 个文件):
> - `src/replicator/Replicator.h`/`.cpp`:归档队列由 `priority_queue<(thread,sequence)>` 改为
>   `std::vector` + `bestArchiveIndex()`,**按"尚未解析的 SCN"排序**(`Parser::orderScn()`,
>   切片过的用切片点 SCN)。这既让两个线程按 SCN 对齐,又使切片后重新入队的归档回到正确的次序。
>   新增 `gapArchives`:归档缺号时不再阻塞整个进程,而是把该归档暂存、继续处理其它线程
>   (仅当无其它候选时才睡眠+重读清单),避免一个线程缺归档拖住另一个线程。
> - `src/parser/Parser.h`/`.cpp`:新增 `slicePending`/`sliceScn`;在 **LWN 边界**判断切片:
>   `待冲刷提交数 >= slice-pending-commits` **且本线程 SCN 领先于全局低水位**
>   (`lwnScn > commitSorter->getWatermark()`)时,保存位置(`saveFileOffset`)、返回并交还调度器。
>   第二个条件很关键:只有落后线程先跑起来低水位才会动;否则会在每个 LWN 切片,实测碎到 512 B/片。
> - `src/common/Ctx.h`、`src/OpenLogReplicator.cpp`、`documentation/json/1.source.adoc`:新增
>   source 级参数 `slice-pending-commits`(默认 1024,0 = 关闭切片)。
> - `src/reader/Reader.h`/`.cpp`/`ReaderFilesystem.*`:**修复一个独立的真实挂死缺陷** ——
>   Reader 打开文件时 `stat` 拿到的大小可能过期(asmfs 对新文件先返回 0),此时
>   `bufferScan >= fileSize` 恒真 → 读取条件永不成立、`bufferEnd == fileSize` 也不成立 →
>   Reader 在 10 ms 空转且**不再 notify Parser**(Parser 停在 `checkFinished`)→ 整个进程静默停住。
>   实测(seq 50):`fileSize=0`、`bufferEnd=1024`、`numBlocksHeader=12580`。
>   修法两处:(a) 归档(`group == 0`)以 redo 头的块数为准修正 `fileSize`;
>   (b) READ 循环在"位置已到文件大小"时调用新增的 `refreshFileSize()` 重新 stat(兜底在线日志增长)。
>
> 验证(容器内 RAC,两实例 = 两 redo thread,`max-mb` 仍 4096):
> - 交错生效:日志顺序 `seq 116(离线恢复) → seq 50(t2) → seq 117(t1) → seq 51(t2) → seq 117@3.9 MB` ✓;
> - 切片粒度:修正前 378 段、每段 ~512 B;修正后 5 段、单段 ~3.9 MB ✓;
> - 0 次 60043/10017(同一批归档此前必然 OOM),checkpoint 推进到 seq 117 末尾(4701804)✓;
> - **端到端**:Kafka `SYS.OLR_TEST` 主题累计 **3,162,898** 条消息,与压测生成的行数
>   (300 万 + 15 万 + 5000×2)一致 —— 63/206/144 MB 三个大归档与含 5000 个小事务的归档全部完整交付 ✓。
>
> 已知**剩余限制**(需现场评估,短期用定容兜住):切片只在"落后线程有归档可解析"时才能释放内存。
> 若一个实例长时间空闲且不切换日志,其水位不动,领先线程在该 SCN 区间内的**全部已提交事务**
> 仍会滞留(实测 node2 空闲时 5000 个小事务把 4 GB 池占满并触发 10017 —— 现在至少是"明确报错"而非静默挂死)。
> 因此:① `max-mb` 仍需按上一节的公式覆盖"单实例空闲窗口内的事务数";
> ② 生产上建议两个实例都有稳定的归档节奏(空闲实例也会有后台事务与定期切换);
> ③ 后续可考虑"空闲线程水位超时释放"(在实例未关闭但长时间无归档时允许水位前推)。

> **未完成项(2026-09-16 同步;均为开放项,不是已定决策)** —— 原理、参数、判定速查见
> `documentation/doc/RAC事务投递与水位线基础.md`:
> 1. **打开 `idle-thread-release-us`**:测试环境配置未设该项(默认 0 = 关闭),因此"只在一个节点造数
>    +切日志"会呈现"等低水位"的原貌 —— **这是 P0-B 静默停住的正确解释,不是缺陷**。
> 2. **空转期周期 tick**:释放检测与冲刷都只在 Parser 推进 LWN 时跑(`setThreadWatermark` ←
>    `Parser.cpp:1573`;`flushReady` ← `Parser.cpp:1591/1628/1689`),两个实例都安静时谁都不触发
>    ⇒ 需在 `Replicator.cpp` 空转分支(60047 处)周期性调用。**已实现**:Parser 向排序器注册
>    Executor,`CommitSorter::refreshIdle()` + 无参 `flushReady()`。
> 3. **"未对齐恢复"规则**(借鉴 Flink `StatusWatermarkValve.java:99-102,168-170`):恢复中的线程在
>    追上当前低水位前不参与 min,避免"恢复即把低水位按回去"。
> 4. **部分释放(bounded lag)**:给领先量加**时间或字节**上界(不要用 SCN,增速随负载差几个数量级),
>    作为"严格 min ↔ 完全释放"之间的旋钮。
> 5. **记录级水位**:水位目前是 LWN(归档)粒度;改成记录级可减小滞后与内存滞留,但需先确认与
>    checkpoint 恢复的一致性。
>
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
