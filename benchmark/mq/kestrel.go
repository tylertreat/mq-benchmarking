package mq

import (
	"time"

	"github.com/alindeman/go-kestrel"
	"github.com/tylertreat/mq-benchmarking/benchmark"
)

type Kestrel struct {
	handler *benchmark.MessageHandler
	queue   string
	pub     *kestrel.Client
	sub     *kestrel.Client
}

func kestrelReceive(k Kestrel) {
	for {
		message, _ := k.sub.Get(k.queue, 1, 0, 0)
		if len(message) > 0 {
			k.ReceiveMessage(message[0].Data)
		} else {
			time.Sleep(time.Millisecond)
		}
	}
}

func NewKestrel(numberOfMessages int) Kestrel {
	pub := kestrel.NewClient("localhost", 2229)
	sub := kestrel.NewClient("localhost", 2229)

	return Kestrel{
		handler: &benchmark.MessageHandler{NumberOfMessages: numberOfMessages},
		queue:   "transient_events",
		pub:     pub,
		sub:     sub,
	}
}

func (k Kestrel) Setup() {
	k.pub.FlushAllQueues()
	go kestrelReceive(k)
}

func (k Kestrel) Teardown() {
	k.pub.Close()
	k.sub.Close()
}

func (k Kestrel) Send(message []byte) {
	k.pub.Put(k.queue, [][]byte{message})
}

func (k Kestrel) ReceiveMessage(message []byte) {
	k.handler.ReceiveMessage(message)
}

func (k Kestrel) MessageHandler() *benchmark.MessageHandler {
	return k.handler
}
