package server

import (
	"distributed-chat-system/internal/message"
	"net/http"

	"distributed-chat-system/internal/connection"
	"distributed-chat-system/internal/pubsub"
	"distributed-chat-system/internal/storage"
	"encoding/json"
	"github.com/gorilla/websocket"
	"time"
)

var serverID = "server-1"

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

var manager = connection.NewManager()
var redisClient = pubsub.NewRedis()

func init() {
	redisClient.Subscribe("chat:"+serverID, func(data []byte) {
		var msg message.Message
		json.Unmarshal(data, &msg)

		targets := manager.Get(msg.To)

		for _, conn := range targets {
			conn.Send <- data
		}
	})
}

func Upgrade(w http.ResponseWriter, r *http.Request) (*websocket.Conn, error) {
	return upgrader.Upgrade(w, r, nil)
}

func ReadLoop(conn *connection.Connection) {
	defer func() {
		manager.Remove(conn.UserID, conn)
		conn.Socket.Close()
		close(conn.Send)

		redisClient.RemoveUserServer(conn.UserID, serverID)

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
		// set basic fields
		msg.From = sender.UserID
		msg.Origin = serverID

		// compute conversation id
		conversationID := getConversationID(msg.From, msg.To)
		msg.ConversationID = conversationID

		// get sequence from redis
		seq, err := redisClient.GetNextSequence(conversationID)
		if err != nil {
			return
		}
		msg.SequenceNumber = seq

		// save message
		msgID := store.SaveMessage(msg.From, msg.To, msg.Body, msg.ConversationID, msg.ClientMsgID, int64(msg.SequenceNumber), )
		msg.MessageID = msgID

		out, _ := json.Marshal(msg)

		// publish to redis
		redisClient.Publish("chat", out)

		// also deliver locally
		targetServers := redisClient.GetUserServers(msg.To)

		if len(targetServers) == 0 {
			// user offline -> already stored
			return
		}

		for _, srv := range targetServers {
			if srv == serverID {
				// local delivery
				targets := manager.Get(msg.To)
				for _, conn := range targets {
					conn.Send <- out
				}
				continue
			}

			// remote delivery
			redisClient.Publish("chat:"+srv, out)
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

	redisClient.AddUserServer(conn.UserID, serverID)

	go startHeartbeat(conn.UserID)

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
			ConversationID: m.ConversationID,
			SequenceNumber: m.SequenceNumber,
			Origin:    "storage",
		}
		data, _ := json.Marshal(outMsg)

		conn.Send <- data

	}

}

var store = storage.NewStore()

func startHeartbeat(userID string) {
	ticker := time.NewTicker(10 * time.Second)

	for range ticker.C {
		redisClient.AddUserServer(userID, serverID)
	}
}

// helper function for handleIncoming
func getConversationID(a, b string) string {
	if a < b {
		return a + ":" + b
	}
	return b + ":" + a
}
