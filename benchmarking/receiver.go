package benchmarking

import "fmt"
import "time"

type Receiver func([]byte)

type MessageReceiver interface {
    Receive(Receiver)
}

type MessageReceivingMachine struct {
    Receiver MessageReceiver
    NumberOfMessages int
    hasStarted bool
    hasCompleted bool
    messageCounter int
    started int64
    stopped int64
}

func NewReceivingMachine(msgReceiver MessageReceiver, messages int) *MessageReceivingMachine {
    msgReceiver.Receive(receiveMessage)
    receivingMachine := &MessageReceivingMachine{
        Receiver:msgReceiver,
        NumberOfMessages:messages,
    }

    return receivingMachine
}

func (receivingMachine MessageReceivingMachine) receiveMessage(message []byte) {
    if !receivingMachine.hasStarted {
        receivingMachine.hasStarted = true
        receivingMachine.started = time.Now().UnixNano()
    }

    receivingMachine.messageCounter++

    if receivingMachine.messageCounter == receivingMachine.NumberOfMessages {
        receivingMachine.stopped = time.Now().UnixNano()
        fmt.Println(receivingMachine.stopped - receivingMachine.started)
        receivingMachine.hasCompleted = true
    }
}

func (receivingMachine MessageReceivingMachine) WaitForCompletion() {
    for {
        if receivingMachine.hasCompleted {
            break
        } else {
            time.Sleep(100)
        }
    }
}

