package server

import (
	"distributed-chat-system/internal/message"
	"net/http"

	"github.com/gorilla/websocket"
	"encoding/json"
	"distributed-chat-system/internal/connection"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func Upgrade(w http.ResponseWriter, r *http.Request) (*websocket.Conn, error) {
	return upgrader.Upgrade(w, r, nil)
}

func ReadLoop(conn *connection.Connection, manager *connection.Manager) {
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
		msg.From = sender.UserID
		out, _ := json.Marshal(msg)

		targets := manager.Get(msg.To)

		for _, conn := range targets {
			conn.Send <- out
		}
	}
}

func HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	_ = conn
}
