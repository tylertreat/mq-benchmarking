package mq

import (
	"github.com/apcera/nats"
	"github.com/tylertreat/mq-benchmarking/benchmark"
)

type Gnatsd struct {
	handler *benchmark.MessageHandler
	conn    *nats.Conn
	subject string
}

func NewGnatsd(numberOfMessages int) Gnatsd {
	conn, _ := nats.Connect(nats.DefaultURL)

	return Gnatsd{
		handler: &benchmark.MessageHandler{NumberOfMessages: numberOfMessages},
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
}

func (g Gnatsd) ReceiveMessage(message []byte) bool {
	return g.handler.ReceiveMessage(message)
}

func (g Gnatsd) MessageHandler() *benchmark.MessageHandler {
	return g.handler
}
