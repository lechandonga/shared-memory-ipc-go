package shmipc

import (
	"os"
	"strconv"
	"sync/atomic"
	"syscall"
)

// crashHookEnv names an environment variable used ONLY by tests. When set to
// a positive integer N, every Nth publication self-kills the process with
// SIGKILL right after the slot is marked INFLIGHT but before payload and
// publication header are written. This deterministically simulates a producer
// dying mid-write without any cleanup handlers running.
const crashHookEnv = "SHMIPC_CRASH_EVERY"

// crashAfterInflight is called from publish between claiming a slot and
// writing its payload.
func crashAfterInflight(nth *uint64) {
	v := os.Getenv(crashHookEnv)
	if v == "" {
		return
	}
	every, err := strconv.ParseUint(v, 10, 64)
	if err != nil || every == 0 {
		return
	}
	if atomic.AddUint64(nth, 1)%every == 0 {
		syscall.Kill(os.Getpid(), syscall.SIGKILL)
	}
}
