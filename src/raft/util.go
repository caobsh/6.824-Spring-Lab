package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const (
	Debug     = false
	HearbeatP = 100
	RandomP   = 200
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func stableTime() time.Duration {
	return HearbeatP * time.Millisecond
}

func randTime() time.Duration {
	return time.Duration(rand.Int63n(RandomP)+HearbeatP*3) * time.Millisecond
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
