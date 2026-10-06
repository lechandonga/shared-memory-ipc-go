//go:build !linux || (!amd64 && !arm64)

package shmipc

import (
	"errors"
	"os"
)

const futexWakeAll = 0x7fffffff

var errUnsupported = errors.New("shmipc: supported only on linux/amd64 and linux/arm64")

func createFile(path string, size int) (*os.File, error) {
	return nil, errUnsupported
}
func openFile(path string) (*os.File, error)         { return nil, errUnsupported }
func mapRegion(f *os.File, size int) (region, error) { return nil, errUnsupported }
func unmapRegion(r region) error                     { return errUnsupported }
