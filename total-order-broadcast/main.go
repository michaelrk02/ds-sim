package main

import (
	"bufio"
	"container/list"
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type message struct {
	sender int
	t int64
	data string
}

type nodePool struct {
	participants int
	broadcast func(m message, lmin, lmax int)

	aliveCount atomic.Int64
}

func newNodePool(participants int, broadcast func(m message, lmin, lmax int)) *nodePool {
	pool := new(nodePool)
	pool.participants = participants
	pool.broadcast = broadcast
	pool.aliveCount.Store(0)
	return pool
}

type node struct {
	pool *nodePool
	id int
	clockSpeed int
	l *log.Logger

	// temporarily store broadcasted message in the staging area
	primaryBuffer *list.List
	secondaryBuffer *list.List
	bufferMu sync.Mutex

	// will wait for all nodes to synchronize
	// drawback: last messages may not be delivered (due to unfinished synchronization)
	tWait int64
	tWaitMu sync.Mutex

	broadcast chan message

	running atomic.Bool

	// lamport timestamp
	t int64
	tMu sync.Mutex
}

func newNode(pool *nodePool, id, clockSpeed int, l *log.Logger) *node {
	n := new(node)
	n.pool = pool
	n.id = id
	n.clockSpeed = clockSpeed
	n.l = l
	n.primaryBuffer = list.New()
	n.secondaryBuffer = list.New()
	n.tWait = 0
	n.broadcast = make(chan message)
	n.running.Store(false)
	n.t = 0

	return n
}

func (n *node) run() {
	n.running.Store(true)

	go func() {
		n.pool.aliveCount.Add(1)
		n.l.Printf("Node %d started at %dms clock speed", n.id, n.clockSpeed)
		for n.running.Load() {
			n.tMu.Lock()
			n.t++
			n.tMu.Unlock()

			time.Sleep(time.Duration(n.clockSpeed) * time.Millisecond)
		}
		n.l.Printf("Node %d stopping", n.id)
		n.pool.aliveCount.Add(-1)
	}()

	// poll broadcast messages
	go func() {
		for n.running.Load() {
			var ok bool

			m, ok := <-n.broadcast
			if ok {
				n.receive(m)
			}
		}
	}()
}

func (n *node) stop() {
	n.running.Store(false)
}

func (n *node) send(data string, lmin, lmax int) {
	n.tMu.Lock()
	t := n.t
	n.tMu.Unlock()

	m := message{
		sender: n.id,
		t: t,
		data: data,
	}

	n.l.Printf("Node %d sends broadcast at %d", n.id, t)

	n.pool.broadcast(m, lmin, lmax)
}

func (n *node) receive(m message) {
	// sync lamport timestamp
	n.tMu.Lock()
	if m.t > n.t {
		n.t = m.t
	}
	n.t++
	n.tMu.Unlock()

	n.queue(m)

	if n.synchronized() {
		// deliver messages in the primary buffer
		n.flush()
	}
}

func (n *node) queue(m message) {
	n.bufferMu.Lock()

	var target *list.List
	n.tWaitMu.Lock()
	if n.primaryBuffer.Len() > 0 {
		if m.t < n.tWait {
			target = n.primaryBuffer // store in the primary buffer if the message is older than wait value
		} else {
			target = n.secondaryBuffer // store in the secondary buffer if the message is newer than (or equal to) wait value
		}
	} else {
		n.tWait = m.t
		target = n.primaryBuffer // store first message in the primary buffer
	}
	n.tWaitMu.Unlock()

	mark := target.Front()
	for mark != nil {
		// total ordering of lamport timestamp
		if m.t < mark.Value.(message).t {
			break
		}
		mark = mark.Next()
	}

	if mark != nil {
		target.InsertBefore(m, mark)
	} else {
		target.PushBack(m)
	}

	n.bufferMu.Unlock()
}

func (n *node) synchronized() bool {
	// ensure all nodes are mentioned in secondary buffer (i.e. no more old messages to wait)

	n.bufferMu.Lock()
	nodes := make(map[int]bool)
	for e := n.secondaryBuffer.Front(); e != nil; e = e.Next() {
		m := e.Value.(message)

		nodes[m.sender] = true
	}
	n.bufferMu.Unlock()

	return len(nodes) == n.pool.participants
}

func (n *node) flush() {
	n.bufferMu.Lock()

	// flush the primary buffer to the network
	for n.primaryBuffer.Front() != nil {
		m := n.primaryBuffer.Remove(n.primaryBuffer.Front()).(message)

		n.tMu.Lock()
		n.t++
		n.l.Printf("Node %d #%d receives broadcast: %s (from node %d at #%d)", n.id, n.t, m.data, m.sender, m.t)
		n.tMu.Unlock()
	}

	// flush the secondary buffer to the primary buffer
	n.tWaitMu.Lock()
	for n.secondaryBuffer.Front() != nil {
		m := n.secondaryBuffer.Remove(n.secondaryBuffer.Front()).(message)
		if m.t > n.tWait {
			n.tWait = m.t
		}
		n.primaryBuffer.PushBack(m)
	}
	n.tWaitMu.Unlock()

	n.bufferMu.Unlock()
}

func main() {
	var logBuilder strings.Builder

	l := log.New(&logBuilder, " [LOG] ", log.LstdFlags)

	var nodeCount int
	fmt.Printf("Number of nodes: ")
	fmt.Scanf("%d", &nodeCount)

	networkJam := make([][]int, nodeCount)
	for i := range networkJam {
		networkJam[i] = make([]int, nodeCount)
		for j := range networkJam[i] {
			networkJam[i][j] = 0
		}
	}

	nodes := make([]*node, nodeCount)
	broadcaster := func(m message, lmin, lmax int) {
		for i := range nodes {
			go func(i int) {
				// broadcast delay (+ network jam)
				r, _ := rand.Int(rand.Reader, big.NewInt(int64(lmax - lmin)))
				latency := int64(networkJam[m.sender][i]) + int64(lmin) + r.Int64()
				time.Sleep(time.Duration(latency) * time.Millisecond)

				nodes[i].broadcast <- m
			}(i)
		}
	}

	pool := newNodePool(nodeCount, broadcaster)
	for i := 0; i < nodeCount; i++ {
		r, _ := rand.Int(rand.Reader, big.NewInt(500))
		clockSpeed := int(500 + r.Int64())

		nodes[i] = newNode(pool, i, clockSpeed, l)
		nodes[i].run()
	}

	for {
		var cmd string
		fmt.Println("Commands: state, broadcast, jam, logs, exit")
		fmt.Printf(" > ")
		fmt.Scanf("%s", &cmd)

		if cmd == "state" {
			for i := range nodes {
				nodes[i].tMu.Lock()
				nodes[i].tWaitMu.Lock()
				nodes[i].bufferMu.Lock()

				fmt.Printf("Node %d (t: %d, tWait: %d, primary: %d, secondary: %d)\n", nodes[i].id, nodes[i].t, nodes[i].tWait, nodes[i].primaryBuffer.Len(), nodes[i].secondaryBuffer.Len())

				nodes[i].bufferMu.Unlock()
				nodes[i].tWaitMu.Unlock()
				nodes[i].tMu.Unlock()
			}
		} else if cmd == "broadcast" {
			var sender int
			var data string
			var lmin, lmax int

			fmt.Printf("Sender: ")
			fmt.Scanf("%d", &sender)
			fmt.Printf("Data: ")
			fmt.Scanf("%s", &data)
			fmt.Printf("Min latency (ms): ")
			fmt.Scanf("%d", &lmin)
			fmt.Printf("Max latency (ms): ")
			fmt.Scanf("%d", &lmax)

			nodes[sender].send(data, lmin, lmax)
		} else if cmd == "jam" {
			// simulate network jam (to ensure total ordering of timestamp works)

			var source, target, latency int

			fmt.Printf("Source node: ")
			fmt.Scanf("%d", &source)
			fmt.Printf("Target node: ")
			fmt.Scanf("%d", &target)
			fmt.Printf("Base latency (ms): ")
			fmt.Scanf("%d", &latency)

			networkJam[source][target] = latency

			fmt.Println("Network jam has been set")
		} else if cmd == "logs" {
			bufio.NewReader(strings.NewReader(logBuilder.String())).WriteTo(os.Stdout)
			logBuilder.Reset()
		} else if cmd == "exit" {
			fmt.Println("Bye")
			break
		}
	}

	for i := range nodes {
		nodes[i].stop()
	}

	fmt.Println("Waiting all nodes to shut down")
	for pool.aliveCount.Load() > 0 {
	}

	bufio.NewReader(strings.NewReader(logBuilder.String())).WriteTo(os.Stdout)
}

