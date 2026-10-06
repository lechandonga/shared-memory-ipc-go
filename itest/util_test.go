package itest

import "fmt"

func msgBody(i int) string {
	return fmt.Sprintf("seq-body-%06d", i)
}
