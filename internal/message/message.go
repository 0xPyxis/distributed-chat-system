package message

type Message struct {
	Type string `json:"type"`
	From string `json:"from"`
	To string `json:"to"`
	Body string `json:"body"`
	MessageID int `json:"message_id"`
	Origin string `json:"origin"`
	ConversationID string `json:"conversation_id"`
	SequenceNumber int64 `json:"sequence_number"`
}