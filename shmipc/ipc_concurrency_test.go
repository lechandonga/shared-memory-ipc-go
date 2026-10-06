package shmipc_test

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/shared-memory-ipc-go/shmipc"
)

type atomicBool struct{ v int32 }

func (a *atomicBool) set()      { atomic.StoreInt32(&a.v, 1) }
func (a *atomicBool) get() bool { return atomic.LoadInt32(&a.v) != 0 }

// Business 1: multiple independent processes send and receive concurrently.
func TestBusinessMultiProcessMPMC(t *testing.T) {
	log := newBusinessLogger(t)
	log.Business("multi-process MPMC concurrent send/receive")

	cfg := shmipc.Config{Capacity: 256, PayloadSize: 128, FullPolicy: shmipc.FullPolicyBlock}
	name := "biz-mpmc"
	creator, norm, err := shmipc.Create(name, cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer creator.Destroy()
	log.Info("created channel %q capacity=%d payload=%d", name, norm.Capacity, norm.PayloadSize)

	bin := buildHelper(t)
	tcfg := testCfg{capacity: norm.Capacity, payload: norm.PayloadSize}

	const producers, consumers, perProc = 3, 3, 200
	total := producers * perProc
	log.Case(fmt.Sprintf("%d producer processes x %d msgs + %d in-process competing consumers => %d exactly-once deliveries",
		producers, perProc, consumers, total))

	var pwg sync.WaitGroup
	for i := 0; i < producers; i++ {
		pwg.Add(1)
		go func(i int) {
			defer pwg.Done()
			args := commonArgs(name, tcfg, "sender",
				"-count", fmt.Sprint(perProc), "-start", fmt.Sprint(i*perProc),
				"-timeout-ms", "5000", "-block")
			p := startHelper(t, bin, args, nil)
			if err := p.wait(t); err != nil {
				t.Errorf("producer %d: %v stderr=%s", i, err, p.stderr.String())
			}
		}(i)
	}

	// Competing consumers each attach independently; they share the cursor
	// in shared memory. Any delivered message must reach exactly one of them.
	stop := make(chan struct{})
	recvCh := make(chan string, total)
	var cwg sync.WaitGroup
	var dup atomicBool
	for i := 0; i < consumers; i++ {
		cwg.Add(1)
		go func() {
			defer cwg.Done()
			sub, aerr := shmipc.Attach(name, norm)
			if aerr != nil {
				t.Errorf("attach consumer: %v", aerr)
				return
			}
			defer sub.Detach()
			for {
				select {
				case <-stop:
					return
				default:
				}
				m, _, rerr := sub.Recv(300 * time.Millisecond)
				if rerr != nil || m == nil {
					continue
				}
				select {
				case recvCh <- string(m):
				case <-stop:
					return
				}
			}
		}()
	}

	pwg.Wait()

	seen := make(map[string]bool, total)
	deadline := time.Now().Add(30 * time.Second)
	for len(seen) < total && time.Now().Before(deadline) {
		select {
		case s := <-recvCh:
			if seen[s] {
				dup.set()
			}
			seen[s] = true
		case <-time.After(500 * time.Millisecond):
		}
	}
	close(stop)
	cwg.Wait()

	if dup.get() {
		t.Fatal("duplicate delivery across competing consumers")
	}
	if len(seen) != total {
		t.Fatalf("delivered=%d want=%d (dup/loss)", len(seen), total)
	}
	log.Pass(fmt.Sprintf("all %d messages from %d processes delivered exactly once to %d competing consumers",
		total, producers, consumers))

	log.Case("attach + non-blocking empty + timeout distinction")
	a, err := shmipc.Attach(name, norm)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Detach()
	if _, _, err := a.Recv(0); !errors.Is(err, shmipc.ErrEmpty) {
		t.Fatalf("want ErrEmpty, got %v", err)
	}
	start := time.Now()
	_, _, err = a.Recv(100 * time.Millisecond)
	if !errors.Is(err, shmipc.ErrTimeout) || time.Since(start) < 80*time.Millisecond {
		t.Fatalf("want ErrTimeout after ~100ms, got %v", err)
	}
	log.Info("non-blocking empty and bounded timeout behave distinctly")

	log.Case("incompatible attach is rejected with distinguishable reason")
	if _, err := shmipc.Attach(name, shmipc.Config{Capacity: 16, PayloadSize: 128}); !errors.Is(err, shmipc.ReasonCapacityMismatch) {
		t.Fatalf("want ReasonCapacityMismatch, got %v", err)
	}
	if _, err := shmipc.Attach(name, shmipc.Config{Capacity: norm.Capacity, PayloadSize: 64}); !errors.Is(err, shmipc.ReasonPayloadMismatch) {
		t.Fatalf("want ReasonPayloadMismatch, got %v", err)
	}
	log.Info("capacity and payload mismatches rejected with specific reasons")
	log.Pass("incompatible attach cases")
}
