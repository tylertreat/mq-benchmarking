package benchmark

import (
	"fmt"
	"time"
)

type MessageSender interface {
	Send([]byte)
}

type MessageSendingMachine struct {
	MessageSender MessageSender
}

func (machine MessageSendingMachine) Run(messageSize int, numberToSend int) {
	message := make([]byte, messageSize)
	start := time.Now().UnixNano()
	for i := 0; i < numberToSend; i++ {
		machine.MessageSender.Send(message)
	}

	stop := time.Now().UnixNano()
	ms := float32(stop-start) / 1000000
	fmt.Printf("Sent %d messages in %f ms\n", numberToSend, ms)
	fmt.Printf("Sent %f per second\n", 1000*float32(numberToSend)/ms)
}
