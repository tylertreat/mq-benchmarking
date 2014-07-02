package benchmark

type inproc struct {
	handler          *MessageHandler
	numberOfMessages int
}

func NewInproc(numberOfMessages int) inproc {
	return inproc{handler: &MessageHandler{NumberOfMessages: numberOfMessages}}
}

func (inproc inproc) Send(message []byte) {
	inproc.ReceiveMessage(message)
}

func (inproc inproc) ReceiveMessage(message []byte) {
	inproc.handler.ReceiveMessage(message)
}

func (inproc inproc) MessageHandler() *MessageHandler {
	return inproc.handler
}
