package mq

import "github.com/tylertreat/mq-benchmarking/benchmark"
import "github.com/pebbe/zmq4"

type zeromq struct {
	handler          *benchmark.MessageHandler
	numberOfMessages int
	sender           *zmq4.Socket
	receiver         *zmq4.Socket
}

func receive(zeromq zeromq) {
	for {
		message, _ := zeromq.receiver.RecvBytes(zmq4.DONTWAIT)
		zeromq.ReceiveMessage(message)
	}
}

func NewZeromq(numberOfMessages int) zeromq {
	ctx, _ := zmq4.NewContext()
	pub, _ := ctx.NewSocket(zmq4.PUB)
	pub.Bind("tcp://*:5555")
	sub, _ := ctx.NewSocket(zmq4.SUB)
	sub.SetSubscribe("")
	sub.Connect("tcp://localhost:5555")

	return zeromq{
		handler:  &benchmark.MessageHandler{NumberOfMessages: numberOfMessages},
		sender:   pub,
		receiver: sub,
	}
}

func (zeromq zeromq) Setup() {
	go receive(zeromq)
}

func (zeromq zeromq) Teardown() {
	//zeromq.context.Term()
	zeromq.sender.Close()
	zeromq.receiver.Close()
}

func (zeromq zeromq) Send(message []byte) {
	zeromq.sender.SendBytes(message, zmq4.DONTWAIT)
}

func (zeromq zeromq) ReceiveMessage(message []byte) {
	zeromq.handler.ReceiveMessage(message)
}

func (zeromq zeromq) MessageHandler() *benchmark.MessageHandler {
	return zeromq.handler
}
