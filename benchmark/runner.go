package benchmark

type Runner struct {
	MessageReceiver MessageReceiver
	MessageSender   MessageSender
}

func (runner Runner) TestThroughput(messageSize int, numberOfMessages int) {
	receiver := NewReceiveEndpoint(runner.MessageReceiver, numberOfMessages)
	sender := &SendEndpoint{MessageSender: runner.MessageSender}
	sender.TestThroughput(messageSize, numberOfMessages)
	receiver.WaitForCompletion()
}

func (runner Runner) TestLatency(messageSize int, numberOfMessages int) {
	receiver := NewReceiveEndpoint(runner.MessageReceiver, numberOfMessages)
	sender := &SendEndpoint{MessageSender: runner.MessageSender}
	sender.TestLatency(messageSize, numberOfMessages)
	receiver.WaitForCompletion()
}
