package mq

import "github.com/tylertreat/mq-benchmarking/benchmark"
import "github.com/pebbe/zmq4"

type Zeromq struct {
	handler  *benchmark.MessageHandler
	sender   *zmq4.Socket
	receiver *zmq4.Socket
}

func zeromqReceive(zeromq Zeromq) {
	for {
		message, _ := zeromq.receiver.RecvBytes(zmq4.DONTWAIT)
		zeromq.ReceiveMessage(message)
	}
}

func NewZeromq(numberOfMessages int) Zeromq {
	ctx, _ := zmq4.NewContext()
	pub, _ := ctx.NewSocket(zmq4.PUB)
	pub.Bind("tcp://*:5555")
	sub, _ := ctx.NewSocket(zmq4.SUB)
	sub.SetSubscribe("")
	sub.Connect("tcp://localhost:5555")

	return Zeromq{
		handler:  &benchmark.MessageHandler{NumberOfMessages: numberOfMessages},
		sender:   pub,
		receiver: sub,
	}
}

func (zeromq Zeromq) Setup() {
	go zeromqReceive(zeromq)
}

func (zeromq Zeromq) Teardown() {
	zeromq.sender.Close()
	zeromq.receiver.Close()
}

func (zeromq Zeromq) Send(message []byte) {
	zeromq.sender.SendBytes(message, zmq4.DONTWAIT)
}

func (zeromq Zeromq) ReceiveMessage(message []byte) {
	zeromq.handler.ReceiveMessage(message)
}

func (zeromq Zeromq) MessageHandler() *benchmark.MessageHandler {
	return zeromq.handler
}
