package mq

import (
	"github.com/apcera/nats"
	"github.com/tylertreat/mq-benchmarking/benchmark"
)

type Gnatsd struct {
	handler benchmark.MessageHandler
	conn    *nats.Conn
	subject string
}

func NewGnatsd(numberOfMessages int, testLatency bool) Gnatsd {
	conn, _ := nats.Connect(nats.DefaultURL)

	var handler benchmark.MessageHandler
	if testLatency {
		handler = &benchmark.LatencyMessageHandler{
			NumberOfMessages: numberOfMessages,
			Latencies:        []float32{},
		}
	} else {
		handler = &benchmark.ThroughputMessageHandler{NumberOfMessages: numberOfMessages}
	}

	return Gnatsd{
		handler: handler,
		subject: "test",
		conn:    conn,
	}
}

func (g Gnatsd) Setup() {
	g.conn.Subscribe(g.subject, func(message *nats.Msg) {
		g.ReceiveMessage(message.Data)
	})
}

func (g Gnatsd) Teardown() {
	g.conn.Close()
}

func (g Gnatsd) Send(message []byte) {
	g.conn.Publish(g.subject, message)
	g.conn.Flush()
}

func (g Gnatsd) ReceiveMessage(message []byte) bool {
	return g.handler.ReceiveMessage(message)
}

func (g Gnatsd) MessageHandler() *benchmark.MessageHandler {
	return &g.handler
}
