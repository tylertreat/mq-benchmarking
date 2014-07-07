package mq

import (
	"time"

	"github.com/op/go-nanomsg"
	"github.com/tylertreat/mq-benchmarking/benchmark"
)

type Nanomsg struct {
	handler  benchmark.MessageHandler
	sender   *nanomsg.PubSocket
	receiver *nanomsg.SubSocket
}

func nanoReceive(nano Nanomsg) {
	for {
		message, _ := nano.receiver.Recv(nanomsg.DontWait)
		if nano.handler.ReceiveMessage(message) {
			break
		}
	}
}

func NewNanomsg(numberOfMessages int, testLatency bool) Nanomsg {
	pub, _ := nanomsg.NewPubSocket()
	pub.Bind("tcp://*:5555")
	sub, _ := nanomsg.NewSubSocket()
	sub.Subscribe("")
	sub.Connect("tcp://localhost:5555")

	var handler benchmark.MessageHandler
	if testLatency {
		handler = &benchmark.LatencyMessageHandler{
			NumberOfMessages: numberOfMessages,
			Latencies:        []float32{},
		}
	} else {
		handler = &benchmark.ThroughputMessageHandler{NumberOfMessages: numberOfMessages}
	}

	return Nanomsg{
		handler:  handler,
		sender:   pub,
		receiver: sub,
	}
}

func (nano Nanomsg) Setup() {
	time.Sleep(3 * time.Second)
	go nanoReceive(nano)
}

func (nano Nanomsg) Teardown() {
	nano.sender.Close()
	nano.receiver.Close()
}

func (nano Nanomsg) Send(message []byte) {
	nano.sender.Send(message, nanomsg.DontWait)
}

func (nano Nanomsg) MessageHandler() *benchmark.MessageHandler {
	return &nano.handler
}
