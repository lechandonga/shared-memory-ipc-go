package shmipc_test

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// businessLogger mirrors every test's progress both into the test output and
// into ./testlogs/run-<timestamp>.log, making it easy to see which business
// scenarios are covered, which cases each contains, and how they fared.
type businessLogger struct {
	t *testing.T
	f io.Writer
}

func newBusinessLogger(t *testing.T) *businessLogger {
	t.Helper()
	dir := filepath.Join("..", "testlogs")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// One business log per `go test` invocation: reuse the path exported by
	// the first logger in the process (set by scripts/run-tests.sh).
	path := os.Getenv("SHMIPC_TEST_LOG")
	if path == "" {
		path = filepath.Join(dir, "run-"+time.Now().Format("20060102-150405.000000")+".log")
		os.Setenv("SHMIPC_TEST_LOG", path)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	l := &businessLogger{t: t, f: f}
	l.Log("==== run started %s ====", time.Now().Format(time.RFC3339))
	t.Logf("business log: %s", path)
	return l
}

func (l *businessLogger) Business(name string) {
	l.t.Helper()
	l.Log("")
	l.Log("---- business: %s ----", name)
}

func (l *businessLogger) Case(name string) { l.Log("  [case] %s", name) }
func (l *businessLogger) Pass(name string) { l.Log("  [PASS] %s", name) }
func (l *businessLogger) Info(f string, a ...any) {
	l.t.Helper()
	l.Log("         "+f, a...)
}

func (l *businessLogger) Log(f string, a ...any) {
	l.t.Helper()
	line := fmt.Sprintf(f, a...)
	l.t.Log(line)
	fmt.Fprintln(l.f, line)
}
