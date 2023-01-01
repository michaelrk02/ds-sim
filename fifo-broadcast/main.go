package main

import (
	"bufio"
	"container/list"
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type message struct {
	sender int
	sequence int
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

	sendSeq int
	delivered []int
	buffer *list.List
	broadcast chan message

	running atomic.Bool
}

func newNode(pool *nodePool, id, clockSpeed int, l *log.Logger) *node {
	n := new(node)
	n.pool = pool
	n.id = id
	n.clockSpeed = clockSpeed
	n.l = l
	n.sendSeq = 0
	n.delivered = make([]int, pool.participants)
	n.buffer = list.New()
	n.broadcast = make(chan message)
	n.running.Store(false)

	return n
}

func (n *node) run() {
	n.running.Store(true)

	go func() {
		n.pool.aliveCount.Add(1)
		n.l.Printf("Node %d started at %dms clock speed", n.id, n.clockSpeed)
		for n.running.Load() {
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
	m := message{
		sender: n.id,
		sequence: n.sendSeq,
		data: data,
	}

	n.l.Printf("Node %d sends broadcast #%d", n.id, n.sendSeq)
	n.sendSeq++

	n.pool.broadcast(m, lmin, lmax)
}

func (n *node) receive(m message) {
	n.buffer.PushBack(m)
	for {
		var ok bool

		ok = false
		var deliver message
		for e := n.buffer.Front(); e != nil; e = e.Next() {
			deliver = e.Value.(message)
			if deliver.sequence == n.delivered[deliver.sender] {
				ok = true
				n.buffer.Remove(e)
				break
			}
		}

		if !ok {
			break
		}

		n.delivered[deliver.sender]++

		n.l.Printf("Node %d receives broadcast: %s (from node %d)", n.id, deliver.data, deliver.sender)
	}
}

func main() {
	var logBuilder strings.Builder

	l := log.New(&logBuilder, " [LOG] ", log.LstdFlags)

	var nodeCount int
	fmt.Printf("Number of nodes: ")
	fmt.Scanf("%d", &nodeCount)

	nodes := make([]*node, nodeCount)
	broadcaster := func(m message, lmin, lmax int) {
		for i := range nodes {
			go func(i int) {
				// broadcast delay
				r, _ := rand.Int(rand.Reader, big.NewInt(int64(lmax - lmin)))
				latency := int64(lmin) + r.Int64()
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
		fmt.Println("Commands: state, broadcast, logs, exit")
		fmt.Printf(" > ")
		fmt.Scanf("%s", &cmd)

		if cmd == "state" {
			for i := range nodes {
				fmt.Printf("Node %d (seq: %d) ", nodes[i].id, nodes[i].sendSeq)

				delivered := make([]string, len(nodes[i].delivered))
				for j := range delivered {
					delivered[j] = strconv.Itoa(nodes[i].delivered[j])
				}

				fmt.Printf("[%s] \n", strings.Join(delivered, ", "))
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

