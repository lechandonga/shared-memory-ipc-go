package shmipc_test

import (
	"fmt"
	"time"

	"github.com/lechandonga/shared-memory-ipc-go/shmipc"
)

// 演示创建、附加、发送、接收（含缺口识别）与销毁的完整流程。
func ExampleChannel() {
	dir := "/tmp/shmipc-example"

	// 进程 A：创建容量 4、单消息上限 64 字节的通道。
	producer, err := shmipc.Create("demo", shmipc.Options{
		Capacity:       4,
		MaxMessageSize: 64,
		FullPolicy:     shmipc.OverflowDropOldest,
		Wait:           shmipc.WaitFutex,
		Dir:            dir,
	})
	if err != nil {
		panic(err)
	}

	// 进程 B：附加时声明期望参数；不一致会得到 ErrIncompatible*。
	consumer, err := shmipc.AttachIn(dir, "demo", 4, 64)
	if err != nil {
		panic(err)
	}

	// 写入超过容量：最旧消息被覆盖，写入永不阻塞。
	for i := 0; i < 10; i++ {
		producer.Send([]byte(fmt.Sprintf("hello-%d", i)), -1)
	}

	for {
		m, err := consumer.Receive(-1) // 非阻塞
		if err == shmipc.ErrEmpty {
			break
		}
		if err != nil {
			panic(err)
		}
		if m.Skipped > 0 {
			fmt.Printf("注意：错过 %d 条消息\n", m.Skipped)
		}
		fmt.Printf("seq=%d %s\n", m.Seq, m.Data)
	}

	consumer.Detach() // 普通参与者：仅断开，不影响通道
	producer.Destroy()
	time.Sleep(time.Millisecond) // 仅为 godoc 示例稳定性，实际无需等待

	// Output:
	// seq=6 hello-6
	// seq=7 hello-7
	// seq=8 hello-8
	// seq=9 hello-9
}
