package mq

import (
	"github.com/tylertreat/mq-benchmarking/benchmark"
	"gopkg.in/stomp.v1"
)

type Activemq struct {
	handler *benchmark.MessageHandler
	pub     *stomp.Conn
	subConn *stomp.Conn
	sub     *stomp.Subscription
	queue   string
}

func activemqReceive(a Activemq) {
	for {
		message := <-a.sub.C
		if a.ReceiveMessage(message.Body) {
			break
		}
	}
}

func NewActivemq(numberOfMessages int) Activemq {
	queue := "test"
	pub, _ := stomp.Dial("tcp", "localhost:61613", stomp.Options{})
	subConn, _ := stomp.Dial("tcp", "localhost:61613", stomp.Options{})
	sub, _ := subConn.Subscribe(queue, stomp.AckAuto)

	return Activemq{
		handler: &benchmark.MessageHandler{NumberOfMessages: numberOfMessages},
		queue:   queue,
		pub:     pub,
		subConn: subConn,
		sub:     sub,
	}
}

func (a Activemq) Setup() {
	go activemqReceive(a)
}

func (a Activemq) Teardown() {
	a.pub.Disconnect()
	a.subConn.Disconnect()
}

func (a Activemq) Send(message []byte) {
	a.pub.Send(a.queue, "", message, nil)
}

func (a Activemq) ReceiveMessage(message []byte) bool {
	return a.handler.ReceiveMessage(message)
}

func (a Activemq) MessageHandler() *benchmark.MessageHandler {
	return a.handler
}
