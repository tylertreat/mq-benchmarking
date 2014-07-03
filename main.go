package main

import (
	"fmt"
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

func runKestrel(messageCount int, messageSize int) {
	log.Println("Begin kestrel test")
	kestrel := mq.NewKestrel(messageCount)
	kestrel.Setup()
	benchmark.Runner{kestrel, kestrel}.Run(messageSize, messageCount)
	kestrel.Teardown()
	log.Println("End kestrel test")
}

func runKafka(messageCount int, messageSize int) {
	log.Println("Begin kafka test")
	kafka := mq.NewKafka(messageCount)
	kafka.Setup()
	benchmark.Runner{kafka, kafka}.Run(messageSize, messageCount)
	kafka.Teardown()
	log.Println("End kafka test")
}

func main() {
	usage := fmt.Sprintf("usage: %s {inproc|zeromq|nanomsg|kestrel|kafka} [num_messages] [message_size]", os.Args[0])

	if len(os.Args) < 2 {
		log.Print(usage)
		os.Exit(1)
	}

	test := os.Args[1]
	messageCount := 1000000
	messageSize := 1000

	if len(os.Args) > 2 {
		count, err := strconv.Atoi(os.Args[2])
		if err != nil {
			log.Print(usage)
			os.Exit(1)
		}
		messageCount = count
	}

	if len(os.Args) > 3 {
		size, err := strconv.Atoi(os.Args[3])
		if err != nil {
			log.Print(usage)
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
	} else if test == "kestrel" {
		runKestrel(messageCount, messageSize)
	} else if test == "kafka" {
		runKafka(messageCount, messageSize)
	} else {
		log.Print(usage)
		os.Exit(1)
	}
}
