package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/tylertreat/mq-benchmarking/benchmark"
	"github.com/tylertreat/mq-benchmarking/benchmark/mq"
)

func newTester(subject string, testLatency bool, msgCount, msgSize int) *benchmark.Tester {
	var messageSender benchmark.MessageSender
	var messageReceiver benchmark.MessageReceiver

	switch subject {
	case "inproc":
		inproc := mq.NewInproc(msgCount, testLatency)
		messageSender = inproc
		messageReceiver = inproc
	case "zeromq":
		zeromq := mq.NewZeromq(msgCount, testLatency)
		messageSender = zeromq
		messageReceiver = zeromq
	case "nanomsg":
		nanomsg := mq.NewNanomsg(msgCount, testLatency)
		messageSender = nanomsg
		messageReceiver = nanomsg
	case "kestrel":
		kestrel := mq.NewKestrel(msgCount, testLatency)
		messageSender = kestrel
		messageReceiver = kestrel
	case "kafka":
		kafka := mq.NewKafka(msgCount, testLatency)
		messageSender = kafka
		messageReceiver = kafka
	case "rabbitmq":
		rabbitmq := mq.NewRabbitmq(msgCount, testLatency)
		messageSender = rabbitmq
		messageReceiver = rabbitmq
	case "nsq":
		nsq := mq.NewNsq(msgCount, testLatency)
		messageSender = nsq
		messageReceiver = nsq
	case "redis":
		redis := mq.NewRedis(msgCount, testLatency)
		messageSender = redis
		messageReceiver = redis
	case "activemq":
		activemq := mq.NewActivemq(msgCount, testLatency)
		messageSender = activemq
		messageReceiver = activemq
	case "nats":
		gnatsd := mq.NewGnatsd(msgCount, testLatency)
		messageSender = gnatsd
		messageReceiver = gnatsd
	case "beanstalkd":
		beanstalkd := mq.NewBeanstalkd(msgCount, testLatency)
		messageSender = beanstalkd
		messageReceiver = beanstalkd
	case "iris":
		iris := mq.NewIris(msgCount, testLatency)
		messageSender = iris
		messageReceiver = iris
	case "surge":
		surge := mq.NewSurgeMQ(msgCount, testLatency)
		messageSender = surge
		messageReceiver = surge
	default:
		return nil
	}

	return &benchmark.Tester{
		subject,
		msgSize,
		msgCount,
		testLatency,
		messageSender,
		messageReceiver,
	}
}

func parseArgs(usage string) (string, bool, int, int) {

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

	return test, testLatency, messageCount, messageSize
}

func main() {
	usage := fmt.Sprintf(
		"usage: %s "+
			"{"+
			"inproc|"+
			"zeromq|"+
			"nanomsg|"+
			"kestrel|"+
			"kafka|"+
			"rabbitmq|"+
			"nsq|"+
			"redis|"+
			"activemq|"+
			"nats|"+
			"beanstalkd|"+
			"iris"+
			"} "+
			"[test_latency] [num_messages] [message_size]",
		os.Args[0])

	tester := newTester(parseArgs(usage))
	if tester == nil {
		log.Println(usage)
		os.Exit(1)
	}

	tester.Test()
}
