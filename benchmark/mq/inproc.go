package mq

import "github.com/tylertreat/brokerless-mq-benchmarking/benchmark"

type inproc struct {
	handler          *benchmark.MessageHandler
	numberOfMessages int
}

func NewInproc(numberOfMessages int) inproc {
	return inproc{handler: &benchmark.MessageHandler{NumberOfMessages: numberOfMessages}}
}

func (inproc inproc) Send(message []byte) {
	inproc.ReceiveMessage(message)
}

func (inproc inproc) ReceiveMessage(message []byte) {
	inproc.handler.ReceiveMessage(message)
}

func (inproc inproc) MessageHandler() *benchmark.MessageHandler {
	return inproc.handler
}
