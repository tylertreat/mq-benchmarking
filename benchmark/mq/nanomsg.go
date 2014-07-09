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
		// TODO: Some messages come back empty. Is this a slow-consumer problem?
		// Should DontWait be used?
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
	// Sleep is needed to avoid race condition with receiving initial messages.
	time.Sleep(3 * time.Second)
	go nanoReceive(nano)
}

func (nano Nanomsg) Teardown() {
	nano.sender.Close()
	nano.receiver.Close()
}

func (nano Nanomsg) Send(message []byte) {
	// TODO: Should DontWait be used? Possibly overloading consumer.
	nano.sender.Send(message, nanomsg.DontWait)
}

func (nano Nanomsg) MessageHandler() *benchmark.MessageHandler {
	return &nano.handler
}
