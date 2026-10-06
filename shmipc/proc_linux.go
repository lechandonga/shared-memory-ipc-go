//go:build linux && (amd64 || arm64)

package shmipc

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// procStartticks returns field 22 (starttime, in clock ticks since boot) of
// /proc/[pid]/stat. Together with the PID it uniquely identifies a process
// instance, so a reused PID cannot be mistaken for the live owner.
func procStartticks(pid int) uint64 {
	f, err := os.Open("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	if !sc.Scan() {
		return 0
	}
	line := sc.Text()
	// The comm field may contain spaces and parentheses; find its end.
	rp := strings.LastIndexByte(line, ')')
	if rp < 0 {
		return 0
	}
	fields := strings.Fields(line[rp+1:])
	// After comm: state(1) ppid(2) pgrp(3) session(4) tty(5) tpgid(6)
	// flags(7) minflt(8) cminflt(9) majflt(10) cmajflt(11) utime(12)
	// stime(13) cutime(14) cstime(15) priority(16) nice(17) num_threads(18)
	// itrealvalue(19) starttime(20)
	if len(fields) < 20 {
		return 0
	}
	v, _ := strconv.ParseUint(fields[19], 10, 64)
	return v
}

// slotOwnerAlive checks PID + starttime identity against a crashed slot.
func slotOwnerAlive(pid uint32, startTicks uint64) bool {
	if !processAlive(int(pid)) {
		return false
	}
	if startTicks == 0 {
		return true
	}
	return procStartticks(int(pid)) == startTicks
}
