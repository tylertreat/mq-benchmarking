package mq

import (
	"time"

	"github.com/pebbe/zmq4"
	"github.com/tylertreat/mq-benchmarking/benchmark"
)

type Zeromq struct {
	handler  benchmark.MessageHandler
	sender   *zmq4.Socket
	receiver *zmq4.Socket
}

func zeromqReceive(zeromq Zeromq) {
	for {
		message, _ := zeromq.receiver.RecvBytes(zmq4.DONTWAIT)
		if zeromq.handler.ReceiveMessage(message) {
			break
		}
	}
}

func NewZeromq(numberOfMessages int, testLatency bool) Zeromq {
	ctx, _ := zmq4.NewContext()
	pub, _ := ctx.NewSocket(zmq4.PUB)
	pub.Bind("tcp://*:5555")
	sub, _ := ctx.NewSocket(zmq4.SUB)

	var handler benchmark.MessageHandler
	if testLatency {
		handler = &benchmark.LatencyMessageHandler{
			NumberOfMessages: numberOfMessages,
			Latencies:        []float32{},
		}
	} else {
		handler = &benchmark.ThroughputMessageHandler{NumberOfMessages: numberOfMessages}
	}

	return Zeromq{
		handler:  handler,
		sender:   pub,
		receiver: sub,
	}
}

func (zeromq Zeromq) Setup() {
	zeromq.receiver.Connect("tcp://localhost:5555")
	zeromq.receiver.SetSubscribe("")
	time.Sleep(3 * time.Second)
	go zeromqReceive(zeromq)
}

func (zeromq Zeromq) Teardown() {
	zeromq.sender.Close()
	zeromq.receiver.Close()
}

func (zeromq Zeromq) Send(message []byte) {
	zeromq.sender.SendBytes(message, zmq4.DONTWAIT)
}

func (zeromq Zeromq) MessageHandler() *benchmark.MessageHandler {
	return &zeromq.handler
}
