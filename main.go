package main

import (
	"log"
	"os"
	"strconv"

	"github.com/tylertreat/mq-benchmarking/benchmark"
	"github.com/tylertreat/mq-benchmarking/benchmark/mq"
)

func runInproc(messageCount int, messageSize int) {
	log.Println("Begin inproc test")
	inproc := mq.NewInproc(messageCount)
	inproc.Setup()
	benchmark.Runner{inproc, inproc}.Run(messageSize, messageCount)
	inproc.Teardown()
	log.Println("End inproc test")
}

func runZeromq(messageCount int, messageSize int) {
	log.Println("Begin zeromq test")
	zeromq := mq.NewZeromq(messageCount)
	zeromq.Setup()
	benchmark.Runner{zeromq, zeromq}.Run(messageSize, messageCount)
	zeromq.Teardown()
	log.Println("End zeromq test")
}

func runNanomsg(messageCount int, messageSize int) {
	log.Println("Begin nanomsg test")
	nanomsg := mq.NewNanomsg(messageCount)
	nanomsg.Setup()
	benchmark.Runner{nanomsg, nanomsg}.Run(messageSize, messageCount)
	nanomsg.Teardown()
	log.Println("End nanomsg test")
}

func main() {
	if len(os.Args) < 2 {
		log.Printf("usage: %s inproc|zeromq|nanomsg [num_messages] [message_size]", os.Args[0])
		os.Exit(1)
	}

	test := os.Args[1]
	messageCount := 1000000
	messageSize := 1000

	if len(os.Args) > 2 {
		count, err := strconv.Atoi(os.Args[2])
		if err != nil {
			log.Printf("usage: %s inproc|zeromq|nanomsg [num_messages] [message_size]", os.Args[0])
			os.Exit(1)
		}
		messageCount = count
	}

	if len(os.Args) > 3 {
		size, err := strconv.Atoi(os.Args[3])
		if err != nil {
			log.Printf("usage: %s inproc|zeromq|nanomsg [num_messages] [message_size]", os.Args[0])
			os.Exit(1)
		}
		messageSize = size
	}

	if test == "inproc" {
		runInproc(messageCount, messageSize)
	} else if test == "zeromq" {
		runZeromq(messageCount, messageSize)
	} else if test == "nanomsg" {
		runNanomsg(messageCount, messageSize)
	} else {
		log.Printf("usage: %s inproc|zeromq|nanomsg [num_messages] [message_size]", os.Args[0])
		os.Exit(1)
	}
}
