package rds

import (
	"bytes"
	"runtime"
	"strconv"
)

// Obtain go-routine ID.
// It's not the best practice to use goroutine's id but in our case it's justifiable.
func getGoRoutineID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}
