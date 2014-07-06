package benchmark

import (
	"encoding/binary"
	"log"
	"time"
)

type MessageSender interface {
	Send([]byte)
}

type MessageSendingMachine struct {
	MessageSender MessageSender
}

func (machine MessageSendingMachine) TestThroughput(messageSize int, numberToSend int) {
	message := make([]byte, messageSize)
	start := time.Now().UnixNano()
	for i := 0; i < numberToSend; i++ {
		machine.MessageSender.Send(message)
	}

	stop := time.Now().UnixNano()
	ms := float32(stop-start) / 1000000
	log.Printf("Sent %d messages in %f ms\n", numberToSend, ms)
	log.Printf("Sent %f per second\n", 1000*float32(numberToSend)/ms)
}

func (machine MessageSendingMachine) TestLatency(messageSize int, numberToSend int) {
	start := time.Now().UnixNano()
	b := make([]byte, 9)
	for i := 0; i < numberToSend; i++ {
		binary.PutVarint(b, time.Now().UnixNano())
		machine.MessageSender.Send(b)
	}

	stop := time.Now().UnixNano()
	ms := float32(stop-start) / 1000000
	log.Printf("Sent %d messages in %f ms\n", numberToSend, ms)
	log.Printf("Sent %f per second\n", 1000*float32(numberToSend)/ms)
}
