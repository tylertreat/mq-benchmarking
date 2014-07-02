package main

import (
	"github.com/tylertreat/brokerless-mq-benchmarking/benchmark"
	"github.com/tylertreat/brokerless-mq-benchmarking/benchmark/mq"
)

func runInproc() {
	const messageCount = 1000000
	const messageSize = 1000

	inproc := mq.NewInproc(messageCount)
	benchmark.Runner{inproc, inproc}.Run(messageSize, messageCount)
}

func main() {
	runInproc()
}
