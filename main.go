package main

import (
	"fmt"

	"github.com/tylertreat/mq-benchmarking/benchmark"
	"github.com/tylertreat/mq-benchmarking/benchmark/mq"
)

func runInproc() {
	fmt.Println("Begin inproc test")
	const messageCount = 1000000
	const messageSize = 1000

	inproc := mq.NewInproc(messageCount)
	benchmark.Runner{inproc, inproc}.Run(messageSize, messageCount)
	fmt.Println("End inproc test")
}

func runZeromq() {
	fmt.Println("Begin zeromq test")
	const messageCount = 1000000
	const messageSize = 1000

	zeromq := mq.NewZeromq(messageCount)
	zeromq.Setup()
	benchmark.Runner{zeromq, zeromq}.Run(messageSize, messageCount)
	zeromq.Teardown()
	fmt.Println("End zeromq test")
}

func main() {
	runInproc()
	runZeromq()
}
