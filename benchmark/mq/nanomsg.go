package mq

import (
	"github.com/op/go-nanomsg"
	"github.com/tylertreat/mq-benchmarking/benchmark"
)

type nano struct {
	handler          *benchmark.MessageHandler
	numberOfMessages int
	sender           *nanomsg.PubSocket
	receiver         *nanomsg.SubSocket
}

func nanoReceive(nano nano) {
	for {
		message, _ := nano.receiver.Recv(nanomsg.DontWait)
		nano.ReceiveMessage(message)
	}
}

func NewNanomsg(numberOfMessages int) nano {
	pub, _ := nanomsg.NewPubSocket()
	pub.Bind("tcp://*:5555")
	sub, _ := nanomsg.NewSubSocket()
	sub.Subscribe("")
	sub.Connect("tcp://localhost:5555")

	return nano{
		handler:  &benchmark.MessageHandler{NumberOfMessages: numberOfMessages},
		sender:   pub,
		receiver: sub,
	}
}

func (nano nano) Setup() {
	go nanoReceive(nano)
}

func (nano nano) Teardown() {
	nano.sender.Close()
	nano.receiver.Close()
}

func (nano nano) Send(message []byte) {
	nano.sender.Send(message, nanomsg.DontWait)
}

func (nano nano) ReceiveMessage(message []byte) {
	nano.handler.ReceiveMessage(message)
}

func (nano nano) MessageHandler() *benchmark.MessageHandler {
	return nano.handler
}
