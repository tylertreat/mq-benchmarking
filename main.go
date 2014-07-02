package main

import (
	"github.com/tylertreat/mq-benchmarking/benchmark"
	"github.com/tylertreat/mq-benchmarking/benchmark/mq"
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
