package main

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"
)

func requestNTP(t time.Time) (t1, t2, t3 time.Time) {
	var r *big.Int
	var d time.Duration

	t1 = t

	// network delay
	r, _ = rand.Int(rand.Reader, big.NewInt(500))
	d, _ = time.ParseDuration(fmt.Sprintf("1s%dms", r.Int64()))
	time.Sleep(d)

	t2 = time.Now()

	// processing time
	r, _ = rand.Int(rand.Reader, big.NewInt(500))
	d, _ = time.ParseDuration(fmt.Sprintf("1s%dms", r.Int64()))
	time.Sleep(d)

	t3 = time.Now()

	// network delay
	r, _ = rand.Int(rand.Reader, big.NewInt(500))
	d, _ = time.ParseDuration(fmt.Sprintf("1s%dms", r.Int64()))
	time.Sleep(d)

	return
}

func main() {
	var t1, t2, t3, t4 time.Time
	var d time.Duration

	t1 = time.Now()
	fmt.Printf("Time before sync: %s\n", t1.Format(time.RFC3339Nano))

	_, t2, t3 = requestNTP(t1)

	t4 = time.Now()
	fmt.Printf("Time after sync: %s\n", t4.Format(time.RFC3339Nano))

	networkDelay := t4.Sub(t1).Nanoseconds() - t3.Sub(t2).Nanoseconds()

	d, _ = time.ParseDuration(fmt.Sprintf("%sns", networkDelay / 2))
	serverTime := t3.Add(d)

	clockSkew := serverTime.Sub(t4).Nanoseconds()

	fmt.Printf("Round-trip network delay: %dns\n", networkDelay)
	fmt.Printf("Estimated server time: %s\n", serverTime.Format(time.RFC3339Nano))
	fmt.Printf("Estimated clock skew: %dns\n", clockSkew)
}

