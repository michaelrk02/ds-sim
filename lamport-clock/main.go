package main

import (
	"bufio"
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
	t int64
	data string
}

type node struct {
	id int
	clockSpeed int
	l *log.Logger

	t int64
	tMu sync.Mutex
	msgCh chan message

	running atomic.Bool
	freezing atomic.Bool
}

func newNode(id, clockSpeed int, l *log.Logger) *node {
	n := new(node)
	n.id = id
	n.clockSpeed = clockSpeed
	n.l = l
	n.t = 0
	n.msgCh = make(chan message)
	n.running.Store(false)
	n.freezing.Store(false)
	return n
}

func (n *node) run() {
	n.running.Store(true)

	// counter increment
	go func() {
		n.l.Printf("Node %d started at %dms clock speed", n.id, n.clockSpeed)
		for n.running.Load() {
			for n.freezing.Load() {
				// freeze, do nothing
			}

			n.tMu.Lock()
			n.t++
			n.tMu.Unlock()

			time.Sleep(time.Duration(n.clockSpeed) * time.Millisecond)
		}
		n.l.Printf("Node %d shutdown", n.id)
	}()

	// poll messages in separate thread
	go func() {
		for n.running.Load() {
			var ok bool

			m, ok := <-n.msgCh
			if ok {
				n.receiveMessage(m)
			}
		}
	}()
}

func (n *node) freeze(d time.Duration) {
	go func() {
		n.l.Printf("Node %d (#%d) frozen for %v", n.id, n.time(), d)

		n.freezing.Store(true)
		time.Sleep(d)
		n.freezing.Store(false)

		// n.t should not change much
		n.l.Printf("Node %d (#%d) unfreezes", n.id, n.time())
	}()
}

func (n *node) time() int64 {
	n.tMu.Lock()
	t := n.t
	n.tMu.Unlock()
	return t
}

func (n *node) stop() {
	n.running.Store(false)
}

func (n *node) receiveMessage(m message) {
	t1 := n.time()

	n.tMu.Lock()
	if m.t > n.t {
		n.t = m.t
	}
	n.t++
	n.tMu.Unlock()

	t2 := n.time()

	n.l.Printf("Node %d (#%d -> #%d) receives message: %s (#%d)", n.id, t1, t2, m.data, m.t)
}

func (n *node) sendMessage(data string, target *node) {
	n.tMu.Lock()
	m := message{
		t: n.t,
		data: data,
	}
	n.tMu.Unlock()

	n.l.Printf("Node %d (#%d) sends message to node %d", n.id, n.time(), target.id)

	// random delay
	r, _ := rand.Int(rand.Reader, big.NewInt(500))
	time.Sleep(time.Duration(r.Int64()) * time.Millisecond)
	// message sent
	target.msgCh <- m
}

func main() {
	var logBuilder strings.Builder

	l := log.New(&logBuilder, " [LOG] ", log.LstdFlags)

	var nodeCount int
	fmt.Printf("Enter number of nodes: ")
	fmt.Scanf("%d", &nodeCount)

	fmt.Println("Starting nodes ...")
	nodes := make([]*node, nodeCount)
	for i := range nodes {
		r, _ := rand.Int(rand.Reader, big.NewInt(500))
		clockSpeed := int(500 + r.Int64())
		nodes[i] = newNode(i, clockSpeed, l)

		go nodes[i].run()
	}

	for {
		var cmd string
		fmt.Printf("Commands: state, send, logs, freeze, exit\n")
		fmt.Printf(" > ")
		fmt.Scanf("%s", &cmd)

		if cmd == "state" {
			for i := range nodes {
				fmt.Printf("Node %d (#%d)\n", nodes[i].id, nodes[i].time())
			}
		} else if cmd == "send" {
			var source, target int
			var data string

			fmt.Printf("Source: ")
			fmt.Scanf("%d", &source)
			fmt.Printf("Target: ")
			fmt.Scanf("%d", &target)

			fmt.Printf("Data: ")
			fmt.Scanf("%s", &data)

			nodes[source].sendMessage(data, nodes[target])
		} else if cmd == "logs" {
			bufio.NewReader(strings.NewReader(logBuilder.String())).WriteTo(os.Stdout)
			fmt.Println()

			logBuilder.Reset()
		} else if cmd == "freeze" {
			var node int
			fmt.Printf("Node: ")
			fmt.Scanf("%d", &node)

			var duration string
			fmt.Printf("Duration: ")
			fmt.Scanf("%s", &duration)

			d, _ := time.ParseDuration(duration)
			nodes[node].freeze(d)
		} else if cmd == "exit" {
			fmt.Println("Bye")
			break
		} else {
			fmt.Println("Unknown command")
		}
	}

	for i := range nodes {
		nodes[i].stop()
	}
}

