package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/tylertreat/mq-benchmarking/benchmark"
	"github.com/tylertreat/mq-benchmarking/benchmark/mq"
)

// TODO: Make this code more DRY.

func runInproc(messageCount int, messageSize int, testLatency bool) {
	log.Println("Begin inproc test")
	inproc := mq.NewInproc(messageCount, testLatency)
	inproc.Setup()
	if testLatency {
		benchmark.Runner{inproc, inproc}.TestLatency(messageSize, messageCount)
	} else {
		benchmark.Runner{inproc, inproc}.TestThroughput(messageSize, messageCount)
	}
	inproc.Teardown()
	log.Println("End inproc test")
}

func runZeromq(messageCount int, messageSize int, testLatency bool) {
	log.Println("Begin zeromq test")
	zeromq := mq.NewZeromq(messageCount, testLatency)
	zeromq.Setup()
	if testLatency {
		benchmark.Runner{zeromq, zeromq}.TestLatency(messageSize, messageCount)
	} else {
		benchmark.Runner{zeromq, zeromq}.TestThroughput(messageSize, messageCount)
	}
	zeromq.Teardown()
	log.Println("End zeromq test")
}

func runNanomsg(messageCount int, messageSize int, testLatency bool) {
	log.Println("Begin nanomsg test")
	nanomsg := mq.NewNanomsg(messageCount, testLatency)
	nanomsg.Setup()
	if testLatency {
		benchmark.Runner{nanomsg, nanomsg}.TestLatency(messageSize, messageCount)
	} else {
		benchmark.Runner{nanomsg, nanomsg}.TestThroughput(messageSize, messageCount)
	}
	nanomsg.Teardown()
	log.Println("End nanomsg test")
}

func runKestrel(messageCount int, messageSize int, testLatency bool) {
	log.Println("Begin kestrel test")
	kestrel := mq.NewKestrel(messageCount, testLatency)
	kestrel.Setup()
	if testLatency {
		benchmark.Runner{kestrel, kestrel}.TestLatency(messageSize, messageCount)
	} else {
		benchmark.Runner{kestrel, kestrel}.TestThroughput(messageSize, messageCount)
	}
	kestrel.Teardown()
	log.Println("End kestrel test")
}

func runKafka(messageCount int, messageSize int, testLatency bool) {
	log.Println("Begin kafka test")
	kafka := mq.NewKafka(messageCount, testLatency)
	kafka.Setup()
	if testLatency {
		benchmark.Runner{kafka, kafka}.TestLatency(messageSize, messageCount)
	} else {
		benchmark.Runner{kafka, kafka}.TestThroughput(messageSize, messageCount)
	}
	kafka.Teardown()
	log.Println("End kafka test")
}

func runRabbitmq(messageCount int, messageSize int, testLatency bool) {
	log.Println("Begin rabbitmq test")
	rabbitmq := mq.NewRabbitmq(messageCount, testLatency)
	rabbitmq.Setup()
	if testLatency {
		benchmark.Runner{rabbitmq, rabbitmq}.TestLatency(messageSize, messageCount)
	} else {
		benchmark.Runner{rabbitmq, rabbitmq}.TestThroughput(messageSize, messageCount)
	}
	rabbitmq.Teardown()
	log.Println("End rabbitmq test")
}

func runNsq(messageCount int, messageSize int, testLatency bool) {
	log.Println("Begin nsq test")
	nsq := mq.NewNsq(messageCount, testLatency)
	nsq.Setup()
	if testLatency {
		benchmark.Runner{nsq, nsq}.TestLatency(messageSize, messageCount)
	} else {
		benchmark.Runner{nsq, nsq}.TestThroughput(messageSize, messageCount)
	}
	nsq.Teardown()
	log.Println("End nsq test")
}

func runRedis(messageCount int, messageSize int, testLatency bool) {
	log.Println("Begin redis test")
	redis := mq.NewRedis(messageCount, testLatency)
	redis.Setup()
	if testLatency {
		benchmark.Runner{redis, redis}.TestLatency(messageSize, messageCount)
	} else {
		benchmark.Runner{redis, redis}.TestThroughput(messageSize, messageCount)
	}
	redis.Teardown()
	log.Println("End redis test")
}

func runActivemq(messageCount int, messageSize int, testLatency bool) {
	log.Println("Begin activemq test")
	activemq := mq.NewActivemq(messageCount, testLatency)
	activemq.Setup()
	if testLatency {
		benchmark.Runner{activemq, activemq}.TestLatency(messageSize, messageCount)
	} else {
		benchmark.Runner{activemq, activemq}.TestThroughput(messageSize, messageCount)
	}
	activemq.Teardown()
	log.Println("End activemq test")
}

func runGnatsd(messageCount int, messageSize int, testLatency bool) {
	log.Println("Begin gnatsd test")
	gnatsd := mq.NewGnatsd(messageCount, testLatency)
	gnatsd.Setup()
	if testLatency {
		benchmark.Runner{gnatsd, gnatsd}.TestLatency(messageSize, messageCount)
	} else {
		benchmark.Runner{gnatsd, gnatsd}.TestThroughput(messageSize, messageCount)
	}
	gnatsd.Teardown()
	log.Println("End gnatsd test")
}

func main() {
	usage := fmt.Sprintf(
		"usage: %s {inproc|zeromq|nanomsg|kestrel|kafka|rabbitmq|nsq|redis|activemq|gnatsd} [test_latency] [num_messages] [message_size]",
		os.Args[0])

	if len(os.Args) < 2 {
		log.Print(usage)
		os.Exit(1)
	}

	test := os.Args[1]
	messageCount := 1000000
	messageSize := 1000
	testLatency := false

	if len(os.Args) > 2 {
		latency, err := strconv.ParseBool(os.Args[2])
		if err != nil {
			log.Print(usage)
			os.Exit(1)
		}
		testLatency = latency
	}

	if len(os.Args) > 3 {
		count, err := strconv.Atoi(os.Args[3])
		if err != nil {
			log.Print(usage)
			os.Exit(1)
		}
		messageCount = count
	}

	if len(os.Args) > 4 {
		size, err := strconv.Atoi(os.Args[4])
		if err != nil {
			log.Print(usage)
			os.Exit(1)
		}
		messageSize = size
	}

	if test == "inproc" {
		runInproc(messageCount, messageSize, testLatency)
	} else if test == "zeromq" {
		runZeromq(messageCount, messageSize, testLatency)
	} else if test == "nanomsg" {
		runNanomsg(messageCount, messageSize, testLatency)
	} else if test == "kestrel" {
		runKestrel(messageCount, messageSize, testLatency)
	} else if test == "kafka" {
		runKafka(messageCount, messageSize, testLatency)
	} else if test == "rabbitmq" {
		runRabbitmq(messageCount, messageSize, testLatency)
	} else if test == "nsq" {
		runNsq(messageCount, messageSize, testLatency)
	} else if test == "redis" {
		runRedis(messageCount, messageSize, testLatency)
	} else if test == "activemq" {
		runActivemq(messageCount, messageSize, testLatency)
	} else if test == "gnatsd" {
		runGnatsd(messageCount, messageSize, testLatency)
	} else {
		log.Print(usage)
		os.Exit(1)
	}
}
