package benchmark

import (
	"log"
	"time"
)

type MessageReceiver interface {
	ReceiveMessage([]byte)
	MessageHandler() *MessageHandler
	Setup()
	Teardown()
}

type MessageReceivingMachine struct {
	MessageReceiver  MessageReceiver
	NumberOfMessages int
	Handler          *MessageHandler
}

func NewReceivingMachine(receiver MessageReceiver, messages int) *MessageReceivingMachine {
	return &MessageReceivingMachine{
		MessageReceiver:  receiver,
		NumberOfMessages: messages,
		Handler:          receiver.MessageHandler(),
	}
}

type MessageHandler struct {
	hasStarted       bool
	hasCompleted     bool
	messageCounter   int
	NumberOfMessages int
	started          int64
	stopped          int64
}

func (handler *MessageHandler) ReceiveMessage(message []byte) {
	if !handler.hasStarted {
		handler.hasStarted = true
		handler.started = time.Now().UnixNano()
	}

	handler.messageCounter++

	if handler.messageCounter == handler.NumberOfMessages {
		handler.stopped = time.Now().UnixNano()
		handler.hasCompleted = true
		ms := float32(handler.stopped-handler.started) / 1000000.0
		log.Printf("Received %d messages in %f ms\n", handler.NumberOfMessages, ms)
		log.Printf("Received %f per second\n", 1000*float32(handler.NumberOfMessages)/ms)
	}
}

func (machine MessageReceivingMachine) WaitForCompletion() {
	for {
		if machine.Handler.hasCompleted {
			break
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}
