package main

import "github.com/tylertreat/brokerless-mq-benchmarking/benchmark"

func runInproc() {
	const messageCount = 1000000
	const messageSize = 1000

	inproc := benchmark.NewInproc(messageCount)
	benchmark.Runner{inproc, inproc}.Run(messageSize, messageCount)
}

func main() {
	runInproc()
}
