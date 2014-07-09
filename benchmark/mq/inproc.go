package mq

import "github.com/tylertreat/mq-benchmarking/benchmark"

type Inproc struct {
	handler benchmark.MessageHandler
}

func NewInproc(numberOfMessages int, testLatency bool) *Inproc {
	var handler benchmark.MessageHandler
	if testLatency {
		handler = &benchmark.LatencyMessageHandler{
			NumberOfMessages: numberOfMessages,
			Latencies:        []float32{},
		}
	} else {
		handler = &benchmark.ThroughputMessageHandler{NumberOfMessages: numberOfMessages}
	}

	return &Inproc{handler: handler}
}

func (inproc *Inproc) Send(message []byte) {
	inproc.handler.ReceiveMessage(message)
}

func (inproc *Inproc) MessageHandler() *benchmark.MessageHandler {
	return &inproc.handler
}

func (inproc *Inproc) Setup() {}

func (inproc *Inproc) Teardown() {}
