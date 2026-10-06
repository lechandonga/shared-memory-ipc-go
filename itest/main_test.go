package itest

import (
	"log"
	"os"
	"testing"
)

// TestMain 让 helper 子进程只执行 TestHelperProcess；父进程开启
// 带时间戳的标准日志（go test -v 会原样展示）。
func TestMain(m *testing.M) {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	os.Exit(m.Run())
}
