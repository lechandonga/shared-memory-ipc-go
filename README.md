# shared-memory-ipc-go

Linux 上基于本地共享内存的多进程消息通道（Go，零第三方依赖）。

- 命名通道的**创建 / 附加 / 销毁**；附加时校验格式与参数，不兼容会得到可区分的明确错误
- 固定容量下的持续读写：消费跟不上时按策略**阻塞**或**覆盖最旧消息**，读取方可通过 `Message.Skipped` 精确识别缺口
- 多生产者 / 多消费者并发；空、满、超时、关闭结果彼此可区分
- 任何一方异常退出（含 `SIGKILL` / OOM）不会永久阻塞其他参与者：持锁者崩溃会被自动接管，计数可诊断
- 单条消息 CRC32 校验 + 两阶段提交发布：永远不会把覆盖到一半的数据当成有效消息

## 快速开始

```go
import "github.com/lechandonga/shared-memory-ipc-go/shmipc"

// 创建方（通常是“宿主”进程）
ch, err := shmipc.Create("orders", shmipc.Options{
    Capacity:       1024,        // 槽位数
    MaxMessageSize: 4096,        // 单消息字节上限
    FullPolicy:     shmipc.OverflowDropOldest, // 或 OverflowBlock
    Wait:           shmipc.WaitFutex,
    Dir:            "/dev/shm", // 默认即 tmpfs
})

// 其他进程附加；后两个参数 >0 时做兼容性校验
peer, err := shmipc.Attach("orders", 1024, 4096)

// 发送：timeout <0 非阻塞 / 0 永久等待 / >0 限时
err = ch.Send([]byte("payload"), time.Second)

// 接收
m, err := peer.Receive(time.Second)
// m.Seq           全局单调提交序号
// m.Data          消息体拷贝
// m.Skipped       相对上一次接收错过的条数（被覆盖导致）

peer.Detach()   // 普通参与者退出：只断开映射
ch.Destroy()    // 创建方：关闭并删除共享内存文件
```

## 错误一览

| 错误 | 含义 |
| --- | --- |
| `ErrEmpty` | 非阻塞接收时缓冲为空 |
| `ErrFull` | 非阻塞发送且 `OverflowBlock` 下缓冲满 |
| `ErrTimeout` | 限时操作在截止时间前未完成 |
| `ErrClosed` | 通道已关闭（缓冲排空后的接收、关闭后的发送） |
| `ErrMessageTooLarge` | 消息超过创建时的 `MaxMessageSize` |
| `ErrNotExist` / `ErrAlreadyExists` | 通道文件不存在 / 已存在 |
| `ErrIncompatibleMagic` | 后备文件不是本格式（防止静默错读） |
| `ErrIncompatibleVersion` | 内存布局版本不一致 |
| `ErrIncompatibleConfig` | 容量或消息上限与期望不一致 |
| `ErrCorrupted` | 消息 CRC 校验失败（读到的绝不可能是半截数据） |

## 运行测试

```bash
# 包内单测（错误语义、并发正确性、CRC、关闭语义）
go test -race -v ./shmipc

# 多进程集成测试（真实 fork/exec 子进程）
go test -v -timeout 180s ./itest
```

集成测试通过重新执行测试二进制扮演生产者 / 消费者 / 崩溃者，
`-v` 日志中可以看到每个业务、用例与 HELPER 子进程的执行情况，覆盖：

1. **创建/附加兼容性**：正常附加、容量/消息上限/魔数不匹配、不存在、重复创建
2. **多进程 MPMC**：3 生产者 + 2 消费者并发（百万级消息），序号严格有序、零丢失零重复
3. **收发中途进程退出**：生产者被杀、消费者被杀、持锁进程被杀（锁被接管）
4. **持续写入覆盖**：覆盖计数、存活消息有序、慢消费者缺口识别、CRC 防篡改
5. **关闭语义**：阻塞中的接收被关闭事件唤醒、剩余消息排空

## 文档

- [docs/design.md](docs/design.md)：内存布局、附加协议、覆盖与等待策略、崩溃恢复规则、验证方法
