# 共享内存消息通道设计说明

## 1. 总体方案

- 后备存储：一个目录（默认 `/dev/shm`，即 tmpfs）下的定长文件
  `shmipc-<name>.shm`；各进程用 `mmap(MAP_SHARED, PROT_READ|PROT_WRITE)`
  映射同一文件偏移，共享同一物理页。
- 同步原语：
  - 通道级互斥锁保护环形缓冲元数据；
  - 三个 futex 字分别等待“非空”“非满”“关闭”，跨进程睡眠 / 唤醒；
  - 直接通过 `SYS_FUTEX` 系统调用实现，无第三方依赖。
- 消息传递：定长槽环形缓冲（bounded ring），每槽一条消息；
  全局单调提交序号 `tailSeq` 同时充当顺序号与覆盖判据。

选择文件映射而非 `memfd_create` / SysV shm，是因为命名通道天然对应
一个可被 `ls`、`rm`、权限位管理和诊断的文件系统对象；放在 `/dev/shm`
时数据只驻留内存，语义与匿名共享内存一致。

## 2. 内存布局

所有字段小端、定长显式编码，不依赖 Go 结构体填充规则；多版本 Go
编译器与不同进程之间 ABI 稳定。文件大小 = `128 + capacity * slotSize`。

### 2.1 通道头（前 128 字节）

| 偏移 | 长度 | 字段 | 说明 |
| --- | --- | --- | --- |
| 0 | 8 | magic | `0x53484d4950433031`（ASCII `SHMIPC01`） |
| 8 | 4 | version | 布局版本，当前 1 |
| 12 | 4 | flags | bit0 = closed |
| 16 | 8 | capacity | 槽位数 |
| 24 | 4 | slotSize | 单槽总字节数（64 对齐） |
| 28 | 4 | payloadCap | 单条消息最大字节数 |
| 32 | 8 | headSeq | 环形缓冲中最早存活消息序号 |
| 40 | 8 | tailSeq | 下一条消息的提交序号（单调递增） |
| 48 | 8 | droppedMsgs | 被覆盖丢弃的消息数（诊断） |
| 56 | 8 | droppedBytes | 被覆盖丢弃的字节数（诊断） |
| 64 | 8 | abandoned | 持锁者崩溃被接管次数（诊断） |
| 72 | 32 | lock | 自旋/崩溃感知锁（见 §5） |
| 104 | 4 | waitRecv | “非空”futex 字 |
| 108 | 4 | waitSend | “非满”futex 字 |
| 112 | 4 | waitClosed | “关闭”futex 字 |
| 116 | 4 | policy | `OverflowPolicy`（0=DropOldest, 1=Block） |
| 120 | 8 | name | 通道名截断，仅诊断 |

### 2.2 槽布局（每个槽 `slotSize` 字节，64 对齐）

| 偏移 | 长度 | 字段 |
| --- | --- | --- |
| 0 | 4 | state：0=空闲/写入中，1=已提交可读 |
| 4 | 4 | length |
| 8 | 4 | crc32（覆盖 seq + payload） |
| 12 | 4 | 填充 |
| 16 | 8 | seq：提交时的全局序号 |
| 24 | payloadCap | payload |

`slotSize = roundUp64(24 + payloadCap)`。

## 3. 附加协议（防止静默错读）

`Create` 以 `O_CREATE|O_EXCL` 建文件，写满头部并把所有槽 `state=0`。
`Attach` 严格按顺序校验，任一失败立即返回且不执行任何收发：

1. 文件打不开：不存在 → `ErrNotExist`；
2. 文件小于头部：→ `ErrIncompatibleMagic`；
3. magic 不匹配：→ `ErrIncompatibleMagic`；
4. version 不匹配：→ `ErrIncompatibleVersion`；
5. 文件大小与头部 `capacity/slotSize` 推算不一致：→ `ErrIncompatibleConfig`；
6. 调用方声明的期望容量 / 消息上限（>0 时）与头部不一致：
   → `ErrIncompatibleConfig`。

容量、消息上限、满策略都固化在头部：附加方始终以通道实际配置运行，
不会因为“各自信念不同”而错读槽边界。

## 4. 收发、覆盖与等待策略

### 4.1 发送（持锁临界区内）

1. `tail - head >= capacity` 时：
   - `OverflowBlock`：释放锁，在 `waitSend` 上睡眠，被唤醒后重新检查；
   - `OverflowDropOldest`：把最旧槽置 `state=0`、`head++`，
     并累加 `droppedMsgs/droppedBytes`，然后继续写。
2. 目标槽先置 `state=0`（令并发读者立即视其为不可读），写 length、
   payload、crc、seq，**最后**以原子存储发布 `state=1`，再推进 `tailSeq`。
3. 临界区外递增 `waitRecv` 并 `FUTEX_WAKE` 所有接收等待者。

### 4.2 接收（持锁临界区内）

1. `head >= tail`：关闭则 `ErrClosed`，否则在锁外等待 `waitRecv`。
2. 槽必须 `state==1`、`seq==head`、length 合法且 CRC 校验通过；
   任一不满足返回 `ErrCorrupted`，绝不返回半截数据。
3. 多消费者竞争同一个 `head`，每条消息恰好投递给一个 CAS 赢家。
4. **缺口检测在消费者本地完成**：每个接收句柄记录 `expectSeq`；
   - 首次接收以当前序号为基线（不报告历史缺口）；
   - 之后收到的 `seq > expectSeq` 时，`Skipped = seq - expectSeq`，
     精确表示“我”错过的消息条数。
   这样多消费者各自独立，且能感知 DropOldest 造成的覆盖。
5. 接收后置 `state=0`、推进 `headSeq`，唤醒 `waitSend`。

### 4.3 等待策略

- `WaitFutex`（默认）：短暂自旋后 `FUTEX_WAIT` 睡眠（每 5ms 醒来
  做一次锁属主存活检查），省 CPU；
- `WaitSpin`：始终 `Gosched` 自旋，用于极低延迟场景。

futex 遵循“在锁外睡眠、唤醒后重新加锁检查”的标准模式，等待线程
不持有通道锁。每个数据事件与关闭事件都对应一次字递增 + WAKE，
配合超时参数，任何等待路径都有界：`timeout <0` 非阻塞、`==0`
永久等待但仍响应关闭与崩溃接管、`>0` 限时。

## 5. 崩溃恢复规则

### 5.1 普通参与者崩溃

- 它只持有自己的 fd / 映射；内核在进程死亡时自动回收。
- 后备文件与共享状态仍属于通道，其他进程继续工作。
- 等待者的睡眠只依赖共享 futex 字与锁属主检测，不依赖死者执行任何
  清理，因此不会永久阻塞。
- 正常退出用 `Detach()`（仅解除映射）；它**不会**关闭共享通道。

### 5.2 持锁进程崩溃（死在临界区）

锁字后记录 `ownerPid` + `ownerStarttime`（来自 `/proc/<pid>/stat`
第 22 列 starttime，单位 clock tick）。竞争等待者每 ~5ms：

1. `ownerPid` 在 `/proc` 中不存在 → 属主已死，接管；
2. PID 存在但 starttime 不一致 → PID 已被复用，原属主已死，接管；
3. PID 存活且 starttime 一致（可能只是被抢占）→ **绝不**接管，
   避免把活进程的临界区状态撕坏。

接管动作：直接把锁置为已锁并写入自己的 pid/starttime，
`FUTEX_WAKE` 同伴，头部 `abandoned++`，并补发一次
`waitRecv/waitSend` 唤醒（死者可能死在“改完状态、发唤醒前”）。
随后临界区逻辑依据 `headSeq/tailSeq/槽 state` 这些不变量重新判定，
因此无论死在覆盖前还是发布前，状态都保持一致：

- 死在发布 `state=1` 前：槽是 `state=0` 的空闲槽，序号未推进；
- 死在发布后、推进 `tailSeq` 前（同一临界区内，实际不可见中间态）：
  锁接管者要么看到完整提交，要么看到未提交；
- 死在覆盖推进 `headSeq` 后：旧槽被当作可写槽重写，CRC 保护读者。

### 5.3 关闭与销毁

- `Close()` 原子置 `closed` 标志并唤醒三个 futex 字；幂等。
  关闭后发送立即 `ErrClosed`；已缓冲消息仍可完整排空，排空后接收
  才返回 `ErrClosed`（优雅关闭语义）。
- `Destroy()` 关闭、解除映射并删除后备文件。其他仍有映射的进程
  可继续访问已映射页直到自身退出；新的 Attach 得到 `ErrNotExist`。

## 6. 正确性要点小结

- **无半截消息**：state 先置 0、最后发布；seq 与 head 一致性检查；CRC。
- **无重复 / 无残缺**：多消费者在锁内竞争单个 head；槽 state 随读取清零。
- **缺口可诊断**：全局单调序号 + 消费者本地 expectSeq + Skipped。
- **无死锁**：所有等待都在锁外；futex 值每次事件都递增；超时有界；
  锁属主死亡最迟 ~50ms 内被接管。
- **状态不错乱**：owner 仅在“进程消失或 PID 复用”时被判定死亡。

## 7. 验证方法

```bash
go vet ./...
CGO_ENABLED=1 go test -race -count=1 ./shmipc       # 同进程并发与语义
go test -v -count=1 -timeout 180s ./itest           # 多进程集成
```

`itest` 通过重新执行测试二进制（`TestHelperProcess`）拉起真实子进程，
`-v` 日志按业务 / 用例输出结果：

- `TestCreateAttachCompatibility`：6 个附加兼容性用例；
- `TestMultiProcessMPMC`：3 生产者 × 4 秒 + 2 消费者（约 120 万条消息），
  断言 Block 策略下 `dropped==0`、消费者序号严格递增；
- `TestPeerCrashMidFlight`：生产者 / 消费者 / 持锁者分别在运行中
  `SIGKILL`，断言存活方继续推进、锁在约 50ms 内被接管
  （`Stats.LockAbandoned==1`）；
- `TestOverwriteDetected`：100 写 8 槽覆盖、存活消息 92..99 有序、
  慢消费者缺口 `skipped==12`；
- `TestCloseWakesWaiters`：阻塞接收在 ~500ms 内被关闭事件唤醒。

诊断运行中通道：

```bash
ls -l /dev/shm/shmipc-*..shm     # 通道文件与大小 = 128 + cap*slotSize
ch.Stats()                       # head/tail、pending、dropped、abandoned
```
