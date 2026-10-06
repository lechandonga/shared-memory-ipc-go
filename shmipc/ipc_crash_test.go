package shmipc_test

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lechandonga/shared-memory-ipc-go/shmipc"
)

// Business 2: peers crash mid-operation; survivors must not block forever and
// must recover/consume the leftover state with explicit loss signaling.
func TestBusinessCrashRecovery(t *testing.T) {
	log := newBusinessLogger(t)
	log.Business("producer/consumer crash during active traffic")

	bin := buildHelper(t)

	t.Run("producer SIGKILLs mid-publish; readers recover hole", func(t *testing.T) {
		name := "biz-crash-producer"
		cfg := shmipc.Config{Capacity: 64, PayloadSize: 4096, FullPolicy: shmipc.FullPolicyBlock}
		creator, norm, err := shmipc.Create(name, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer creator.Destroy()

		tcfg := testCfg{capacity: norm.Capacity, payload: norm.PayloadSize}
		log.Case("crashed producer leaves INFLIGHT head slot; other process drains without blocking")

		// Helper sends 10 messages, then crashes on its 11th publication
		// (between INFLIGHT and payload copy), deterministically.
		args := commonArgs(name, tcfg, "sender-hook",
			"-count", "100", "-timeout-ms", "2000", "-block")
		p := startHelper(t, bin, args, []string{"SHMIPC_CRASH_EVERY=11"})
		werr := p.wait(t)
		if werr == nil {
			t.Fatalf("expected crash exit, got clean exit: %s", p.stdout.String())
		}
		log.Info("producer exited abnormally as intended")

		// Surviving in-test consumer attached from this process.
		reader, err := shmipc.Attach(name, norm)
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Detach()

		got := 0
		var lostTotal uint64
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			m, ov, err := reader.Recv(200 * time.Millisecond)
			if ov != nil {
				lostTotal += ov.Lost
				log.Info("overflow reported: Lost=%d (Crash gap)", ov.Lost)
			}
			if m != nil {
				if !strings.HasPrefix(string(m), "msg-") {
					t.Fatalf("corrupt message: %q", m)
				}
				got++
			}
			if err == shmipc.ErrTimeout {
				break // nothing more will arrive (crashed producer)
			}
			if err != nil && !errors.Is(err, shmipc.ErrTimeout) {
				t.Fatal(err)
			}
		}
		if got < 10 {
			t.Fatalf("expected >=10 intact messages, got %d", got)
		}
		if lostTotal != 0 {
			// Death happened after head claim but before publish; the
			// message never existed so no overflow is required, but if
			// reported it must be exactly 1.
			if lostTotal > 1 {
				t.Fatalf("unexpected loss count %d", lostTotal)
			}
		}
		log.Pass(fmt.Sprintf("surviving reader got %d well-formed messages and was never blocked", got))

		// A NEW producer must be able to reclaim the poisoned head slot and
		// traffic resumes end-to-end through the same channel.
		args2 := commonArgs(name, tcfg, "sender",
			"-count", "50", "-timeout-ms", "3000", "-block")
		p2 := startHelper(t, bin, args2, nil)
		if err := p2.wait(t); err != nil {
			t.Fatalf("recovery sender: %v stderr=%s", err, p2.stderr.String())
		}
		recovered := 0
		for recovered < 50 {
			m, ov, err := reader.Recv(time.Second)
			if err != nil {
				t.Fatalf("read after recovery: %v", err)
			}
			if ov != nil {
				log.Info("recovery-phase overflow: Lost=%d", ov.Lost)
			}
			if m != nil && strings.HasPrefix(string(m), "msg-") {
				recovered++
			}
		}
		log.Pass("new producer reclaimed poisoned slot; 50 more messages flowed")
	})

	t.Run("consumer SIGKILLs while blocked; producers continue", func(t *testing.T) {
		name := "biz-crash-consumer"
		cfg := shmipc.Config{Capacity: 32, PayloadSize: 128, FullPolicy: shmipc.FullPolicyBlock}
		creator, norm, err := shmipc.Create(name, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer creator.Destroy()
		tcfg := testCfg{capacity: norm.Capacity, payload: norm.PayloadSize}
		log.Case("blocked consumer killed; in-process receiver still gets producer data")

		p := startHelper(t, bin, commonArgs(name, tcfg, "recv-crash"), nil)
		_ = p.wait(t) // SIGKILL exit
		log.Info("blocked consumer killed with SIGKILL")

		reader, err := shmipc.Attach(name, norm)
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Detach()

		// Producer and survivor consumer run concurrently; the blocked
		// queue must continue flowing after the killed consumer is gone.
		args := commonArgs(name, tcfg, "sender",
			"-count", "40", "-timeout-ms", "5000", "-block")
		sp := startHelper(t, bin, args, nil)
		sendErr := make(chan error, 1)
		go func() { sendErr <- sp.wait(t) }()

		for i := 0; i < 40; i++ {
			m, _, err := reader.Recv(2 * time.Second)
			if err != nil {
				t.Fatalf("msg %d: %v", i, err)
			}
			if string(m) != fmt.Sprintf("msg-%08d", i) {
				t.Fatalf("out-of-order/corrupt at %d: %q", i, m)
			}
		}
		if err := <-sendErr; err != nil {
			t.Fatalf("sender after consumer crash: %v stderr=%s", err, sp.stderr.String())
		}
		log.Pass("40 messages received in exact order after peer death; no permanent blockage")
	})

	t.Run("creator dies; channel name is reclaimable", func(t *testing.T) {
		name := "biz-crash-creator"
		cfg := shmipc.Config{Capacity: 16, PayloadSize: 64}
		// Create from a child process and kill it, leaving the backing file.
		tcfg := testCfg{capacity: cfg.Capacity, payload: cfg.PayloadSize}
		cp := startHelper(t, bin,
			append(commonArgs(name, tcfg, "create"), "-count", "0"), nil)
		time.Sleep(200 * time.Millisecond)
		cp.kill()
		cp.wait(t)
		log.Info("creator killed; backing file left in /dev/shm")

		// Existing attaches keep working; but for end-of-life reclaim we
		// verify Create of same name reaps the stale file once all handles
		// are gone (handles are gone after creator death; we never attached).
		ch2, norm2, err := shmipc.Create(name, cfg)
		if err != nil {
			t.Fatalf("recreate after dead creator: %v", err)
		}
		if err := ch2.Send([]byte("after-recreate"), 0); err != nil {
			t.Fatal(err)
		}
		m, _, err := ch2.Recv(0)
		if err != nil || string(m) != "after-recreate" {
			t.Fatalf("recreated channel roundtrip: %q %v", m, err)
		}
		ch2.Destroy()
		log.Pass("stale channel reclaimed and name reused (" + strconv.Itoa(int(norm2.Capacity)) + " slots)")
	})
}
