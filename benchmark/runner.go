package benchmark

type Runner struct {
	MessageReceiver MessageReceiver
	MessageSender   MessageSender
}

func (runner Runner) TestThroughput(messageSize int, numberOfMessages int) {
	receivingMachine := NewReceivingMachine(runner.MessageReceiver, numberOfMessages)
	sendingMachine := &MessageSendingMachine{MessageSender: runner.MessageSender}
	sendingMachine.TestThroughput(messageSize, numberOfMessages)
	receivingMachine.WaitForCompletion()
}

func (runner Runner) TestLatency(messageSize int, numberOfMessages int) {
	receivingMachine := NewReceivingMachine(runner.MessageReceiver, numberOfMessages)
	sendingMachine := &MessageSendingMachine{MessageSender: runner.MessageSender}
	sendingMachine.TestLatency(messageSize, numberOfMessages)
	receivingMachine.WaitForCompletion()
}
