//go:build !linux || (!amd64 && !arm64)

package shmipc

import "time"

func processAlive(pid int) bool                               { return false }
func slotOwnerAlive(pid uint32, t uint64) bool                { return false }
func procStartticks(pid int) uint64                           { return 0 }
func futexWait32(addr *uint32, value uint32, d time.Duration) {}
func futexWake32(addr *uint32, n int)                         {}
func loadClosed(r region) bool                                { return true }
