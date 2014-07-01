package benchmarking

import "fmt"

type Runner struct {
    MessageReceiver MessageReceiver
    MessageSender MessageSender
}

func (runner Runner) Run(messageSize int, messageCount int) {
    fmt.Println(runner.MessageSender)
    receivingMachine := MessageReceivingMachine{
        Receiver:runner.MessageReceiver,
        NumberOfMessages:messageCount,
    }

    sendingMachine := MessageSendingMachine{
        Sender:runner.MessageSender,
        ReportingInterval:1000000,
    }

    sendingMachine.Run(messageSize, messageCount)
    receivingMachine.WaitForCompletion()
}

