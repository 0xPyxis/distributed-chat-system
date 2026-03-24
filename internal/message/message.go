package message

type Message struct {
	Type string `json:"type"`
	From string `json:"from"`
	To string `json:"to"`
	Body string `json:"body"`
	MessageID int `json:"message_id"`
}