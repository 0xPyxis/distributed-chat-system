package server

import (
	"distributed-chat-system/internal/message"
	"net/http"

	"distributed-chat-system/internal/connection"
	"distributed-chat-system/internal/storage"
	"encoding/json"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

var manager = connection.NewManager()

func Upgrade(w http.ResponseWriter, r *http.Request) (*websocket.Conn, error) {
	return upgrader.Upgrade(w, r, nil)
}

func ReadLoop(conn *connection.Connection) {
	defer func() {
		manager.Remove(conn.UserID, conn)
		conn.Socket.Close()

	}()

	for {
		_, data, err := conn.Socket.ReadMessage()
		if err != nil {
			break
		}

		// process message
		handleIncoming(conn, manager, data)
	}
}

func WriteLoop(conn *connection.Connection) {
	for msg := range conn.Send {
		err := conn.Socket.WriteMessage(websocket.TextMessage, msg)
		if err != nil {
			return
		}
	}
}

func handleIncoming(sender *connection.Connection, manager *connection.Manager, data []byte) {
	var msg message.Message
	json.Unmarshal(data, &msg)

	if msg.Type == "message" {
		msgID := store.SaveMessage(sender.UserID, msg.To, msg.Body)
		msg.MessageID = msgID
		msg.From = sender.UserID
		out, _ := json.Marshal(msg)

		targets := manager.Get(msg.To)

		for _, conn := range targets {
			conn.Send <- out
		}
	}

	if msg.Type == "ack" {
		store.MarkDelivered(msg.MessageID)
		return
	}
}

func HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	_, data, err := ws.ReadMessage()
	if err != nil {
		ws.Close()
		return
	}

	var msg message.Message
	json.Unmarshal(data, &msg)

	if msg.Type != "auth" || msg.From == "" {
		ws.Close()
		return
	}

	conn := &connection.Connection{
		UserID: msg.From,
		Socket: ws,
		Send:   make(chan []byte, 256),
	}

	manager.Add(conn.UserID, conn)

	msgs := store.GetUndelivered(conn.UserID)

	go WriteLoop(conn)
	go ReadLoop(conn)

	for _, m := range msgs {
		outMsg := message.Message{
			Type:      "message",
			From:      m.Sender,
			To:        m.Receiver,
			Body:      m.Body,
			MessageID: m.ID,
		}
		data, _ := json.Marshal(outMsg)

		conn.Send <- data

	}

}

var store = storage.NewStore()
