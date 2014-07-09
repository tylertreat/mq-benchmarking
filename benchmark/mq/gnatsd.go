package mq

import (
	"fmt"
	"time"

	"github.com/apcera/nats"
	"github.com/tylertreat/mq-benchmarking/benchmark"
)

type Gnatsd struct {
	handler     benchmark.MessageHandler
	conn        *nats.Conn
	subject     string
	testLatency bool
}

func NewGnatsd(numberOfMessages int, testLatency bool) *Gnatsd {
	conn, _ := nats.Connect(nats.DefaultURL)

	// We want to be alerted if we get disconnected, this will
	// be due to Slow Consumer.
	conn.Opts.AllowReconnect = false

	// Report async errors.
	conn.Opts.AsyncErrorCB = func(nc *nats.Conn, sub *nats.Subscription, err error) {
		panic(fmt.Sprintf("NATS: Received an async error! %v\n", err))
	}

	// Report a disconnect scenario.
	conn.Opts.DisconnectedCB = func(nc *nats.Conn) {
		fmt.Printf("Getting behind! %d\n", nc.OutMsgs-nc.InMsgs)
		panic("NATS: Got disconnected!")
	}

	var handler benchmark.MessageHandler
	if testLatency {
		handler = &benchmark.LatencyMessageHandler{
			NumberOfMessages: numberOfMessages,
			Latencies:        []float32{},
		}
	} else {
		handler = &benchmark.ThroughputMessageHandler{NumberOfMessages: numberOfMessages}
	}

	return &Gnatsd{
		handler:     handler,
		subject:     "test",
		conn:        conn,
		testLatency: testLatency,
	}
}

func (g *Gnatsd) Setup() {
	g.conn.Subscribe(g.subject, func(message *nats.Msg) {
		g.handler.ReceiveMessage(message.Data)
	})
}

func (g *Gnatsd) Teardown() {
	g.conn.Close()
}

const (
	// Maximum bytes we will get behind before we start slowing down publishing.
	maxBytesBehind = 1024 * 1024 // 1MB

	// Maximum msgs we will get behind before we start slowing down publishing.
	maxMsgsBehind = 65536 // 64k

	// Maximum msgs we will get behind when testing latency
	maxLatencyMsgsBehind = 10 // 10

	// Time to delay publishing when we are behind.
	delay = 1 * time.Millisecond
)

func (g *Gnatsd) Send(message []byte) {
	// Check if we are behind by >= 1MB bytes
	bytesDeltaOver := g.conn.OutBytes-g.conn.InBytes >= maxBytesBehind
	// Check if we are behind by >= 65k msgs
	msgsDeltaOver := g.conn.OutMsgs-g.conn.InMsgs >= maxMsgsBehind
	// Override for latency test.
	if g.testLatency {
		msgsDeltaOver = g.conn.OutMsgs-g.conn.InMsgs >= maxLatencyMsgsBehind
	}

	// If we are behind on either condition, sleep a bit to catch up receiver.
	if bytesDeltaOver || msgsDeltaOver {
		time.Sleep(delay)
	}

	g.conn.Publish(g.subject, message)
}

func (g *Gnatsd) MessageHandler() *benchmark.MessageHandler {
	return &g.handler
}
