//go:build linux && arm64

package shmipc

const (
	sysFutex     = 98
	futexWait    = 0
	futexWake    = 1
	futexWakeAll = 0x7fffffff
)
