package mq

import (
	"fmt"
	"time"

	"github.com/kr/beanstalk"
	"github.com/tylertreat/mq-benchmarking/benchmark"
)

type Beanstalkd struct {
	handler benchmark.MessageHandler
	pub     *beanstalk.Conn
	sub     *beanstalk.Conn
}

func beanstalkdReceive(b *Beanstalkd) {
	for {
		id, message, err := b.sub.Reserve(5 * time.Second)
		if err != nil {
			panic(fmt.Sprintf("beanstalkd: Received an error! %v\n", err))
		}

		b.sub.Delete(id)
		if b.handler.ReceiveMessage(message) {
			break
		}
	}
}

func NewBeanstalkd(numberOfMessages int, testLatency bool) *Beanstalkd {
	pub, _ := beanstalk.Dial("tcp", "localhost:11300")
	sub, _ := beanstalk.Dial("tcp", "localhost:11300")

	var handler benchmark.MessageHandler
	if testLatency {
		handler = &benchmark.LatencyMessageHandler{
			NumberOfMessages: numberOfMessages,
			Latencies:        []float32{},
		}
	} else {
		handler = &benchmark.ThroughputMessageHandler{NumberOfMessages: numberOfMessages}
	}

	return &Beanstalkd{
		handler: handler,
		pub:     pub,
		sub:     sub,
	}
}

func (b *Beanstalkd) Setup() {
	go beanstalkdReceive(b)
}

func (b *Beanstalkd) Teardown() {
	b.pub.Close()
	b.sub.Close()
}

func (b *Beanstalkd) Send(message []byte) {
	b.pub.Put(message, 1, 0, 0)
}

func (b *Beanstalkd) MessageHandler() *benchmark.MessageHandler {
	return &b.handler
}
