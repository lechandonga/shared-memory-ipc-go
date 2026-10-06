# shared-memory-ipc-go

同机多进程、基于 POSIX 共享内存与 futex 的多生产者/多消费者消息通道。

- 命名通道的创建 / 附加 / 关闭 / 销毁；附加与创建配置不一致时被**明确拒绝**
  并返回可区分原因（magic / ABI / 容量 / 负载 / 截断 / 损坏）。
- 固定容量持续读写：消费者跟不上时可按 `FullPolicyOverwrite` 覆盖旧数据，
  读者通过 `OverflowError` 精确得知缺失条数，未覆盖消息按序、不重复、不
  残缺送达；`FullPolicyBlock` 下保证不丢消息。
- 多生产者多消费者并发；空 / 满 / 超时 / 关闭结果可区分；容量、负载、满
  策略可配置；无锁，无死锁/状态错乱。
- 任意一方 `SIGKLL` 崩溃后其余进程不被永久阻塞：死在发布中途的槽被识别
  为空洞（PID + 进程启动 tick 双重身份判定，防 PID 复用），可安全接管或
  回收；遗留 `/dev/shm` 文件可被安全重建。

## 快速开始

```go
import "github.com/lechandonga/shared-memory-ipc-go/shmipc"

// 进程 A（创建者）
ch, cfg, err := shmipc.Create("orders", shmipc.Config{
    Capacity:    4096,            // 槽位数
    PayloadSize: 4096,            // 单消息最大字节
    FullPolicy:  shmipc.FullPolicyOverwrite, // 或 FullPolicyBlock
})
if err != nil { log.Fatal(err) }
defer ch.Destroy()

if err := ch.Send([]byte("hello"), time.Second); err != nil {
    log.Fatal(err) // ErrFull / ErrTimeout / ErrClosed / ErrMessageTooLarge
}

// 进程 B（附加者，必须使用同一份 cfg）
sub, err := shmipc.Attach("orders", cfg)
if err != nil { log.Fatal(err) }
defer sub.Detach()

msg, overflow, err := sub.Recv(-1) // 永久等待；0=非阻塞；>0=超时
switch {
case errors.Is(err, shmipc.ErrEmpty):    // 非阻塞且无消息
case errors.Is(err, shmipc.ErrTimeout):  // 超时
case errors.Is(err, shmipc.ErrClosed):   // 通道关闭
case overflow != nil:
    log.Printf("跳过了 %d 条（崩溃导致=%v），当前消息=%q",
        overflow.Lost, overflow.Crash, msg)
case err == nil:
    log.Printf("收到 %q", msg)
}
```

## 文档与测试

- 设计细节见 `docs/DESIGN.md`（内存布局、附加协议、覆盖与等待策略、崩溃
  恢复规则、验证方法）。
- 运行测试：`./scripts/run-tests.sh` 或 `go test ./shmipc -count=1 -v`；
  业务化日志写入 `testlogs/`，可看出覆盖了哪些业务、各业务的用例与结果。
