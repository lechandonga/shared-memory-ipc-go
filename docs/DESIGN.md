# 共享内存消息通道设计说明

本包 `shmipc` 提供**同机多进程**之间基于 POSIX 共享内存（`/dev/shm`）与
`futex(2)` 的多生产者/多消费者（MPMC）定长环形消息通道。仅依赖 Linux
内核接口与 Go 标准库，支持 `linux/amd64` 与 `linux/arm64`。

## 1. 内存布局

通道由 `/dev/shm/shmipc-<name>` 单个文件承载，`mmap(MAP_SHARED)` 到各进程
地址空间。整体由一个 128 字节定长头部和 `Capacity` 个等长槽位组成：

```
+====================  header (128 B)  ====================+
| off  大小 字段            含义                             |
| 0    u32  magic           'SHMC'，0x53484D43                |
| 4    u16  version         ABI 版本（当前 1）                |
| 6    u16  flags           保留                             |
| 8    u64  totalSize       整个映射字节数                    |
| 16   u32  numSlots        容量（槽位数）                    |
| 20   u32  slotSize        单槽字节数（16 对齐）             |
| 24   u32  payloadSize     单条消息最大负载                  |
| 28   u32  headerCRC       静态字段 CRC32-IEEE               |
| 32   u64  writePos        单调递增的写入位置（head）        |
| 40   u64  readPos         单调递增的读取位置（tail）        |
| 48   u32  closed          1=通道已关闭                      |
| 52   u32  fullPolicy      0=阻塞 1=覆盖                     |
| 56   u32  senderFutex     等待空位的 futex 字（4B 对齐）    |
| 60   u32  recverFutex     等待消息的 futex 字（4B 对齐）    |
| 64   u32  creatorPID      创建者 PID                        |
| 72   u64  creatorBoot     创建者进程启动 tick（/proc）       |
| 80   u64  lostCount       被覆盖/崩溃丢弃消息的累计计数     |
+===========================================================+
+==  slot[numSlots]，每个 slotSize 字节（16 对齐）  ==+
| 0   u64  seq        Vyukov 序号（按圈递增）        |
| 8   u32  state      EMPTY/INFLIGHT/READY/ABORTED   |
| 12  u32  dataLen    负载长度                        |
| 16  u32  writerPID  当前写者 PID                    |
| 24  u64  writerTime 写者进程启动 tick（防 PID 复用）|
| 32  u64  (reserved)                                 |
| 40  u64  dataCRC    负载 FNV-1a64 校验和            |
| 48  ... payload    payloadSize 可用字节            |
+=====================================================+
```

所有运行时字段（`writePos/readPos/closed/lostCount`、两个 futex 字、每个
槽的全部字段）都只用 `atomic` 64/32 位读写与 CAS 访问；`headerCRC` 只覆盖
**静态**布局与身份字段（magic/version/尺寸/策略/创建者），因为运行时字段
本来就会持续变化，不能被一次性校验和约束。

## 2. 附加（Attach）协议与兼容性

- `Create(name, cfg)` 以 `O_CREAT|O_EXCL` 原子建文件并初始化；重名返回
  `ErrAlreadyExists`。若同名文件的创建者进程已死亡（PID + 启动 tick 双重
  确认，避免 PID 复用误判），会先安全回收再重建。
- `Attach(name, cfg)` 打开既有通道，按顺序校验：文件大小 → magic →
  ABI 版本 → 槽容量 → 负载大小 → 槽/总大小 → 头部 CRC。任何一项不匹配都
  返回包裹 `ErrIncompatible` 的**具体原因**，调用方用 `errors.Is` 区分：
  - `ReasonBadMagic`：不是本通道（防止把别的 shm 文件当通道错读）；
  - `ReasonBadVersion`：ABI 版本不一致；
  - `ReasonCapacityMismatch` / `ReasonPayloadMismatch` /
    `ReasonSizeMismatch`：配置形状不同；
  - `ReasonTruncated`：文件被截断、映射会越界；
  - `ReasonCorruptHeader`：静态头部损坏。

因此创建方与附加方配置不一致时一定被**明确拒绝**，不会静默错读。

## 3. 入队/出队与无锁正确性

队列采用扩展的 Vyukov 有界 MPMC 序号协议。

- 槽 `i` 的序号按“圈”变化：可写时 `seq == pos`，写完可读时 `seq == pos+1`；
  消费后把 `seq` 推进 `pos + Capacity`，即该槽下一圈的可写序号。
- 生产者先 CAS 抢占 `writePos`（拿到位置 `pos`），只有抢到这一圈的写者才
  会写对应槽；消费者先校验 `seq/state/CRC`，再 CAS 抢占 `readPos`，只有
  抢到的消费者交付消息并把槽序号推进到下一圈。位置号是单调计数器，槽按
  `pos % Capacity` 复用，因此天然有序且每圈每个槽只属于一个位置。
- 槽状态细化发布过程：`EMPTY → INFLIGHT（拷贝负载）→ READY`；负载校验用
  FNV-1a64，覆盖读者若与写者并发，复制后再比对 `seq + state + checksum`，
  任一不符就重试，**绝不会把写一半或新旧拼接的数据当成有效消息**。

## 4. 满队列与覆盖策略

`FullPolicy` 二选一（创建时固定，附加时必须一致）：

- `FullPolicyBlock`（默认）：满时 `Send` 等待空位。非阻塞（timeout=0）
  返回 `ErrFull`；带超时返回 `ErrTimeout`；通道关闭返回 `ErrClosed`。
- `FullPolicyOverwrite`：满时生产者回收队头最旧的槽写入新消息。每次覆盖
  都会把 `readPos` 推进到“仍能容纳在容量窗口内”的位置，并把被丢弃的位置
  数累加进头部 `lostCount`。读者下次 `Recv` 通过 `OverflowError{Lost:n}`
  精确得知缺了多少条；窗口内未被覆盖的消息仍按写入顺序、不重复不残缺地
  送达。对正在 `INFLIGHT` 的活写者有 2ms 有界等待，超时返回 `ErrFull`，
  绝不破坏活写者的数据。

## 5. 等待策略与可区分结果

- 等待用两个独立 futex 字：生产者等 `senderFutex`（消费者释放槽后唤醒），
  消费者等 `recverFutex`（生产者发布/关闭后唤醒）。futex 字是单调递增的
  事件序号，等待者在重检谓词后以当前值进入 `FUTEX_WAIT`，配合最多 50ms
  的等待步长，既避免惊群/漏唤醒导致的永久阻塞，也保证关闭一定能唤醒所有
  等待者（`FUTEX_WAKE` with `INT_MAX`）。
- 结果全部可区分：`ErrEmpty`（非阻塞空）、`ErrFull`（非阻塞满/活写者占用）、
  `ErrTimeout`（超时）、`ErrClosed`（关闭/销毁）、`ErrMessageTooLarge`、
  `OverflowError`（有缺失，但可同时携带下一条有效消息）。
- 容量 `Capacity`（2 ~ 1M）、负载 `PayloadSize`（≤ 4 MiB）与满策略均可配。

## 6. 崩溃恢复规则

- 每个槽在 `INFLIGHT` 时记录写者 PID 与 `/proc/<pid>/stat` 的启动 tick；
  判断写者是否存活用 `kill(pid,0)` + 启动 tick 双重身份，PID 复用也不会
  误判。
- 生产者在拷贝途中被 `SIGKILL`：槽停在 `INFLIGHT`。其他参与者检测到写者
  已死即把槽转为 `ABORTED`（一个空洞）。读者越过空洞时收到
  `OverflowError{Lost:1, Crash:true}`；后续生产者可在同一位置直接接管槽
  继续发布。任何情况下都不会出现因为某个进程消失而让其他进程永久等待。
- 消费者崩溃：它只在 futex 上睡眠、不持有任何锁或槽，死亡对队列状态零
  影响，生产者与其他消费者继续工作。
- 创建者崩溃：遗留的 `/dev/shm` 文件由下一次同名 `Create` 在确认创建者
  进程已死后自动回收；存活期间的 `Attach` 不受影响。`Destroy` 显式删除
  后备文件，`Detach` 仅解除本进程映射。
- `Close` 幂等：置 `closed=1` 并唤醒全部等待者。之后 `Send` 返回
  `ErrClosed`；`Recv` 先排空已发布消息，排空后返回 `ErrClosed`。

## 7. 验证方法

```
# 全部单元/集成测试
go test ./shmipc -count=1 -v

# 带竞态检测（进程内并发）
CGO_ENABLED=1 go test -race ./shmipc -count=1

# 一键脚本（输出同时落盘到 testlogs/）
./scripts/run-tests.sh
```

测试覆盖三大业务，日志（`testlogs/run-<时间戳>.log` 及 `-v` 输出）按
“业务 → 用例 [case] → 结果 [PASS]”组织：

1. **多进程并发收发**：3 个独立生产者进程各发 200 条（编号全局唯一），3
   个附加消费者竞争消费，断言 600 条恰好一次、无重复无丢失；并验证
   空/超时可区分、容量与负载不匹配被具体原因拒绝。
2. **收发中途进程退出**：生产者在 `INFLIGHT` 窗口被 `SIGKILL`（通过测试
   钩子确定性注入），读者识别空洞不阻塞且新生产者能接管；阻塞中的消费者
   被杀死后生产继续、消息严格有序；创建者被杀死后同名通道可回收重建。
3. **持续写入覆盖**：8 倍容量持续写，首条读取精确报告 `Lost=56` 且尾部 8
   条连续有序完整；两个进程高频覆盖写、独立排空者校验每条消息体完整无撕
   裂；并区分满/超时/关闭三种发送结果。
