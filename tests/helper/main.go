//go:build linux && (amd64 || arm64)

// Command testhelper is a child-process worker used by shmipc integration
// tests. It is never meant to be run by humans; the parent test binary
// re-executes itself-as-helper style by building this program explicitly.
//
// Usage:
//
//	testhelper -name CHANNEL -cap N -payload N -mode MODE [extra args]
//
// Modes (see tests/helper/protocol.go):
//
//	send-crash   publish messages then kill self mid-publish window
//	recv-crash   attach as consumer then kill self while blocked
//	sender       produce N messages with a body pattern
//	receiver     consume N messages, write a compact summary line to stdout
//	overwriter   produce continuously until told to stop via stdin EOF
//	drainer      consume continuously until channel closed, print counts
//
package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"syscall"
	"time"

	"github.com/lechandonga/shared-memory-ipc-go/shmipc"
)

func main() {
	name := flag.String("name", "", "channel name")
	capacity := flag.Int("cap", 0, "slot capacity")
	payload := flag.Int("payload", 0, "payload size")
	mode := flag.String("mode", "", "worker mode")
	count := flag.Int("count", 0, "message count")
	timeoutMs := flag.Int("timeout-ms", 0, "per-op timeout in ms")
	overwrite := flag.Bool("overwrite", false, "use FullPolicyOverwrite")
	startIdx := flag.Int("start", 0, "first message index")
	crashAt := flag.Int("crash-at", 0, "kill self after this many bytes (send-crash)")
	_ = crashAt
	block := flag.Bool("block", false, "use blocking policy waits")
	flag.Parse()

	cfg := shmipc.Config{
		Capacity:    uint32(*capacity),
		PayloadSize: uint32(*payload),
	}
	if *overwrite {
		cfg.FullPolicy = shmipc.FullPolicyOverwrite
	}

	var ch *shmipc.Channel
	var err error
	if mode2 := *mode; mode2 == "create" {
		ch, cfg, err = shmipc.Create(*name, cfg)
	} else {
		ch, err = shmipc.Attach(*name, cfg)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "open failed: %v\n", err)
		os.Exit(2)
	}

	timeout := time.Duration(*timeoutMs) * time.Millisecond
	if *block {
		timeout = -1
	}

	switch *mode {
	case "create":
		// Creator just holds the channel open and waits for stdin EOF, then
		// destroys it.
		ioWait()
		ch.Destroy()

	case "sender", "sender-hook":
		// sender-hook runs under SHMIPC_CRASH_EVERY exported by parent.
		runSender(ch, *count, timeout, *startIdx)

	case "receiver":
		runReceiver(ch, *count, timeout)

	case "receiver-shared":
		// Compete with sibling consumers until the shared total is drained.
		// Each process stops once it sees the channel drained AND a quiet
		// window with no new messages; the parent closes the channel to end
		// the last waiter.
		runReceiverShared(ch, *count, timeout)

	case "send-crash":
		runSendCrash(ch, *count, *crashAt)

	case "recv-crash":
		runRecvCrash(ch, timeout)

	case "overwriter":
		runOverwriter(ch, timeout)

	case "drainer":
		runDrainer(ch)

	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q\n", *mode)
		os.Exit(2)
	}
}

func ioWait() {
	buf := make([]byte, 16)
	for {
		if _, err := os.Stdin.Read(buf); err != nil {
			return
		}
	}
}

func body(i int) []byte {
	return []byte(fmt.Sprintf("msg-%08d", i))
}

func runSender(ch *shmipc.Channel, count int, timeout time.Duration, start int) {
	var sent int
	for i := 0; i < count; i++ {
		if err := ch.Send(body(start+i), timeout); err != nil {
			fmt.Fprintf(os.Stderr, "send %d: %v\n", i, err)
			os.Exit(3)
		}
		sent++
	}
	fmt.Printf("sender sent=%d\n", sent)
}

func runReceiver(ch *shmipc.Channel, count int, timeout time.Duration) {
	var got, lost int
	want := make(map[string]bool)
	for got < count {
		// A per-call timeout long enough for normal flow but bounded so a
		// competitor stealing the last message cannot hang us forever.
		wait := timeout
		if wait <= 0 || wait > 15*time.Second {
			wait = 15 * time.Second
		}
		m, ov, err := ch.Recv(wait)
		if ov != nil {
			lost += int(ov.Lost)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "recv: %v (got=%d lost=%d)\n", err, got, lost)
			os.Exit(4)
		}
		if m == nil {
			continue
		}
		s := string(m)
		if want[s] {
			fmt.Fprintf(os.Stderr, "duplicate %s\n", s)
			os.Exit(5)
		}
		want[s] = true
		got++
	}
	fmt.Printf("receiver got=%d lost=%d\n", got, lost)
}

// runReceiverShared competes for messages until the channel is closed (all
// producers finished and parent closed it), then reports its share.
func runReceiverShared(ch *shmipc.Channel, count int, timeout time.Duration) {
	var got, lost int
	seen := make(map[string]bool)
	for {
		m, ov, err := ch.Recv(2 * time.Second)
		if ov != nil {
			lost += int(ov.Lost)
		}
		if err == shmipc.ErrClosed {
			break
		}
		if err != nil {
			continue
		}
		if m == nil {
			continue
		}
		s := string(m)
		if seen[s] {
			fmt.Fprintf(os.Stderr, "duplicate %s\n", s)
			os.Exit(5)
		}
		seen[s] = true
		got++
	}
	fmt.Printf("receiver got=%d lost=%d\n", got, lost)
}

// runSendCrash publishes messages normally, then performs a raw write while
// holding a slot claimed and kills the process with SIGKILL so cleanup cannot
// run. We approximate "mid-publish crash" by writing a large message and
// self-killing at a chosen offset using a helper goroutine — the channel code
// itself claims head then copies; instead we force the window deterministically
// by sending, then immediately SIGKILL from a timer set to fire during a tight
// burst. For deterministic slot poisoning see runSendCrashRaw.
func runSendCrash(ch *shmipc.Channel, count, crashAt int) {
	for i := 0; i < count; i++ {
		if err := ch.Send(body(i), 0); err != nil {
			fmt.Fprintf(os.Stderr, "pre-send: %v\n", err)
			os.Exit(3)
		}
	}
	// Fill the ring and then die hard while blocked/overwriting so state is
	// left INFLIGHT if the kill lands mid-publish.
	go func() {
		time.Sleep(time.Duration(crashAt) * time.Millisecond)
		syscall.Kill(os.Getpid(), syscall.SIGKILL)
	}()
	big := make([]byte, 4096)
	for i := 0; ; i++ {
		_ = ch.Send(big, 0)
	}
}

func runRecvCrash(ch *shmipc.Channel, timeout time.Duration) {
	go func() {
		time.Sleep(20 * time.Millisecond)
		syscall.Kill(os.Getpid(), syscall.SIGKILL)
	}()
	_, _, _ = ch.Recv(-1)
	os.Exit(0)
}

func runOverwriter(ch *shmipc.Channel, timeout time.Duration) {
	// Signal readiness then write forever until killed (parent kills after
	// enough churn). Body includes a sequence number for later inspection.
	fmt.Printf("overwriter ready pid=%d\n", os.Getpid())
	var i int
	for {
		b := []byte(fmt.Sprintf("ov-%012d-%s", i, pad()))
		if err := ch.Send(b, 0); err != nil {
			// Overwrite policy should never return full in steady state;
			// tolerate closed.
			if err == shmipc.ErrClosed {
				return
			}
		}
		i++
	}
}

func pad() string {
	b := make([]byte, 64)
	for i := range b {
		b[i] = byte('a' + i%26)
	}
	return string(b)
}

func runDrainer(ch *shmipc.Channel) {
	var got, lost, closed int
	for {
		m, ov, err := ch.Recv(-1)
		if ov != nil {
			lost += int(ov.Lost)
		}
		if m != nil {
			got++
		}
		if err == shmipc.ErrClosed {
			closed = 1
			break
		}
	}
	fmt.Printf("drainer got=%d lost=%d closed=%d\n", got, lost, closed)
}

var _ = strconv.Itoa
