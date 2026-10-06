//go:build linux && amd64

package shmipc

// See futex(2). Relative-time waits keep the implementation independent of
// the host's FUTEX_CLOCK_REALTIME/CLOCK_MONOTONIC configuration.
const (
	sysFutex     = 202
	futexWait    = 0
	futexWake    = 1
	futexWakeAll = 0x7fffffff
)
