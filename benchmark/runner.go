package benchmark

type Runner struct {
	MessageReceiver MessageReceiver
	MessageSender   MessageSender
}

func (runner Runner) Run(messageSize int, numberOfMessages int) {
	receivingMachine := NewReceivingMachine(runner.MessageReceiver, numberOfMessages)
	sendingMachine := &MessageSendingMachine{MessageSender: runner.MessageSender}
	sendingMachine.Run(messageSize, numberOfMessages)
	receivingMachine.WaitForCompletion()
}
