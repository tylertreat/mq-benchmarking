package mq

import "github.com/tylertreat/mq-benchmarking/benchmark"

type Inproc struct {
	handler *benchmark.MessageHandler
}

func NewInproc(numberOfMessages int) Inproc {
	return Inproc{handler: &benchmark.MessageHandler{NumberOfMessages: numberOfMessages}}
}

func (inproc Inproc) Send(message []byte) {
	inproc.ReceiveMessage(message)
}

func (inproc Inproc) ReceiveMessage(message []byte) {
	inproc.handler.ReceiveMessage(message)
}

func (inproc Inproc) MessageHandler() *benchmark.MessageHandler {
	return inproc.handler
}

func (inproc Inproc) Setup() {}

func (inproc Inproc) Teardown() {}
