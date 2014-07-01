package main

import "github.com/tylertreat/brokerless-mq-benchmarking/benchmarking"

func runInproc() {
    const messageCount = 1000000
    const messageSize = 1000

    inproc := &benchmarking.Inproc{}
    benchmarking.Runner{inproc, inproc}.Run(messageSize, messageCount)
}

func main() {
    runInproc()
}

