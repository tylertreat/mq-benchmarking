package mq

import (
	"github.com/apcera/nats"
	"github.com/tylertreat/mq-benchmarking/benchmark"
)

type Gnatsd struct {
	handler *benchmark.MessageHandler
	pub     *nats.Conn
	sub     *nats.Conn
	subject string
}

func NewGnatsd(numberOfMessages int) Gnatsd {
	pub, _ := nats.Connect(nats.DefaultURL)
	sub, _ := nats.Connect(nats.DefaultURL)

	return Gnatsd{
		handler: &benchmark.MessageHandler{NumberOfMessages: numberOfMessages},
		subject: "test",
		pub:     pub,
		sub:     sub,
	}
}

func (g Gnatsd) Setup() {
	g.sub.Subscribe(g.subject, func(message *nats.Msg) {
		g.ReceiveMessage(message.Data)
	})
}

func (g Gnatsd) Teardown() {
	g.pub.Close()
	g.sub.Close()
}

func (g Gnatsd) Send(message []byte) {
	g.pub.Publish(g.subject, message)
}

func (g Gnatsd) ReceiveMessage(message []byte) bool {
	return g.handler.ReceiveMessage(message)
}

func (g Gnatsd) MessageHandler() *benchmark.MessageHandler {
	return g.handler
}
