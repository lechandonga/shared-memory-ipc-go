package shmipc

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func newTestChannel(t *testing.T, cap uint64, maxMsg uint32, policy OverflowPolicy) *Channel {
	t.Helper()
	dir := t.TempDir()
	c, err := Create("t", Options{
		Capacity: cap, MaxMessageSize: maxMsg, Dir: dir, FullPolicy: policy,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Destroy() })
	return c
}

// 业务：错误类型可区分——空、满、超时、过大消息。
func TestSentinelErrors(t *testing.T) {
	c := newTestChannel(t, 2, 16, OverflowBlock)

	if _, err := c.Receive(-1); !errors.Is(err, ErrEmpty) {
		t.Fatalf("want ErrEmpty, got %v", err)
	}
	t.Log("用例1 通过: 非阻塞接收空通道 => ErrEmpty")

	if err := c.Send(make([]byte, 17), -1); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("want ErrMessageTooLarge, got %v", err)
	}
	t.Log("用例2 通过: 超长消息 => ErrMessageTooLarge")

	c.Send([]byte("a"), -1)
	c.Send([]byte("b"), -1)
	if err := c.Send([]byte("c"), -1); !errors.Is(err, ErrFull) {
		t.Fatalf("want ErrFull, got %v", err)
	}
	t.Log("用例3 通过: Block 策略满缓冲非阻塞发送 => ErrFull")

	for {
		if _, err := c.Receive(-1); err != nil {
			if errors.Is(err, ErrEmpty) {
				break
			}
			t.Fatalf("排空失败: %v", err)
		}
	}
	if _, err := c.Receive(80 * time.Millisecond); !errors.Is(err, ErrTimeout) {
		t.Fatalf("want ErrTimeout, got %v", err)
	}
	t.Log("用例4 通过: 等待超时 => ErrTimeout")
}

// 业务：同进程多 goroutine 并发收发的严格正确性。
// DropOldest 下：所有存活消息有序；Block 下：消息总数精确匹配、无重复。
func TestConcurrentInProcess(t *testing.T) {
	t.Run("Block策略_总量精确", func(t *testing.T) {
		c := newTestChannel(t, 32, 64, OverflowBlock)
		const producers, perP, consumers = 4, 500, 4
		var wg sync.WaitGroup
		for p := 0; p < producers; p++ {
			wg.Add(1)
			go func(pid int) {
				defer wg.Done()
				for i := 0; i < perP; i++ {
					if err := c.Send([]byte(fmt.Sprintf("p%d-%d", pid, i)), 5*time.Second); err != nil {
						t.Errorf("send: %v", err)
						return
					}
				}
			}(p)
		}
		var mu sync.Mutex
		counts := make(map[uint64]int)
		var consumerWg sync.WaitGroup
		for k := 0; k < consumers; k++ {
			consumerWg.Add(1)
			go func() {
				defer consumerWg.Done()
				local := 0
				for local < producers*perP/consumers {
					m, err := c.Receive(50 * time.Millisecond)
					if errors.Is(err, ErrTimeout) {
						if muLockTotal(&mu, counts) >= producers*perP {
							return
						}
						continue
					}
					if err != nil {
						t.Errorf("recv: %v", err)
						return
					}
					mu.Lock()
					counts[m.Seq]++
					mu.Unlock()
					local++
				}
			}()
		}
		wg.Wait()
		consumerWg.Wait()
		total := muLockTotal(&mu, counts)
		if total != producers*perP {
			t.Fatalf("收到 %d, 期望 %d", total, producers*perP)
		}
		for seq, n := range counts {
			if n != 1 {
				t.Fatalf("seq %d 被投递 %d 次（要求恰好一次）", seq, n)
			}
		}
		t.Logf("用例通过: %d 生产者 x %d 消息 / %d 消费者，%d 条消息恰好一次、无重复",
			producers, perP, consumers, total)
	})

	t.Run("DropOldest策略_存活消息有序", func(t *testing.T) {
		c := newTestChannel(t, 8, 64, OverflowDropOldest)
		for i := 0; i < 200; i++ {
			c.Send([]byte(fmt.Sprintf("m-%d", i)), -1)
		}
		var last uint64
		n := 0
		for {
			m, err := c.Receive(-1)
			if errors.Is(err, ErrEmpty) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if n > 0 && m.Seq != last+1 {
				t.Fatalf("顺序断裂: %d -> %d", last, m.Seq)
			}
			want := []byte(fmt.Sprintf("m-%d", m.Seq))
			if !bytes.Equal(m.Data, want) {
				t.Fatalf("内容不匹配 seq=%d", m.Seq)
			}
			last = m.Seq
			n++
		}
		if n != 8 {
			t.Fatalf("存活 %d 条, 期望 8", n)
		}
		t.Logf("用例通过: 覆盖后 %d 条存活消息严格有序且内容一致，dropped=%d",
			n, c.Stats().DroppedMsgs)
	})
}

func muLockTotal(mu *sync.Mutex, counts map[uint64]int) int {
	mu.Lock()
	defer mu.Unlock()
	return len(counts)
}

// 业务：CRC 校验——直接篡改槽负载必须被识别为 ErrCorrupted，
// 绝不把写坏的数据当成正常消息返回。
func TestCRCDetectsTamper(t *testing.T) {
	c := newTestChannel(t, 4, 32, OverflowDropOldest)
	c.Send([]byte("integrity"), -1)
	slot := c.slotAddr(0)
	slot[slotHeaderSize] ^= 0xFF // 翻转负载首字节
	_, err := c.Receive(-1)
	if !errors.Is(err, ErrCorrupted) {
		t.Fatalf("want ErrCorrupted, got %v", err)
	}
	t.Log("用例通过: 负载被篡改 => ErrCorrupted（拒绝返回坏消息）")
}

// 业务：关闭后排空剩余消息，再收到 ErrClosed；关闭幂等。
func TestCloseDrainAndClosed(t *testing.T) {
	c := newTestChannel(t, 4, 16, OverflowBlock)
	c.Send([]byte("x"), -1)
	c.Send([]byte("y"), -1)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("重复 Close 应幂等: %v", err)
	}
	if m, err := c.Receive(-1); err != nil || string(m.Data) != "x" {
		t.Fatalf("排空1: %v %q", err, m.Data)
	}
	if m, err := c.Receive(-1); err != nil || string(m.Data) != "y" {
		t.Fatalf("排空2: %v %q", err, m.Data)
	}
	if _, err := c.Receive(-1); !errors.Is(err, ErrClosed) {
		t.Fatalf("排空后应 ErrClosed, got %v", err)
	}
	if err := c.Send([]byte("z"), -1); !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后发送应 ErrClosed, got %v", err)
	}
	t.Log("用例通过: 关闭后剩余消息可排空，之后收发均返回 ErrClosed")
}
