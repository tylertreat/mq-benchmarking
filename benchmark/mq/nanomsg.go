package mq

import (
	"github.com/op/go-nanomsg"
	"github.com/tylertreat/mq-benchmarking/benchmark"
)

type Nanomsg struct {
	handler  *benchmark.MessageHandler
	sender   *nanomsg.PubSocket
	receiver *nanomsg.SubSocket
}

func nanoReceive(nano Nanomsg) {
	for {
		message, _ := nano.receiver.Recv(nanomsg.DontWait)
		if nano.ReceiveMessage(message) {
			break
		}
	}
}

func NewNanomsg(numberOfMessages int) Nanomsg {
	pub, _ := nanomsg.NewPubSocket()
	pub.Bind("tcp://*:5555")
	sub, _ := nanomsg.NewSubSocket()
	sub.Subscribe("")
	sub.Connect("tcp://localhost:5555")

	return Nanomsg{
		handler:  &benchmark.MessageHandler{NumberOfMessages: numberOfMessages},
		sender:   pub,
		receiver: sub,
	}
}

func (nano Nanomsg) Setup() {
	go nanoReceive(nano)
}

func (nano Nanomsg) Teardown() {
	nano.sender.Close()
	nano.receiver.Close()
}

func (nano Nanomsg) Send(message []byte) {
	nano.sender.Send(message, nanomsg.DontWait)
}

func (nano Nanomsg) ReceiveMessage(message []byte) bool {
	return nano.handler.ReceiveMessage(message)
}

func (nano Nanomsg) MessageHandler() *benchmark.MessageHandler {
	return nano.handler
}
