package server

import (
	"distributed-chat-system/internal/connection"
	"distributed-chat-system/internal/logger"
	"distributed-chat-system/internal/message"
	"distributed-chat-system/internal/metrics"
	"distributed-chat-system/internal/pubsub"
	"distributed-chat-system/internal/storage"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
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
		if err := json.Unmarshal(data, &msg); err != nil {
			logger.Log.Error("failed to unmarshal message",
				zap.Error(err),
			)
			return
		}

		targets := manager.Get(msg.To)

		for _, conn := range targets {
			select {
			case conn.Send <- data:
			default:
				logger.Log.Warn("backpressure drop (subscriber)",
					zap.String("user", msg.To),
				)
				conn.Socket.Close()
				manager.Remove(conn.UserID, conn)
			}
		}
	})

	redisClient.Subscribe("typing", func(data []byte) {
		var msg message.Message
		json.Unmarshal(data, &msg)

		members := store.GetConversationMembers(msg.ConversationID)

		for _, user := range members {
			if user == msg.From {
				continue
			}

			targets := manager.Get(user)

			for _, conn := range targets {
				select {
				case conn.Send <- data:
				default:
					conn.Socket.Close()
					manager.Remove(conn.UserID, conn)
				}
			}
		}
	})

}

func Upgrade(w http.ResponseWriter, r *http.Request) (*websocket.Conn, error) {
	return upgrader.Upgrade(w, r, nil)
}

func ReadLoop(conn *connection.Connection) {
	defer func() {
		logger.Log.Info("connection closed",
			zap.String("user", conn.UserID),
		)

		metrics.ActiveConnections.Dec()

		conn.Socket.Close()
		manager.Remove(conn.UserID, conn)
		close(conn.Send)
		redisClient.RemoveUserServer(conn.UserID, serverID)
	}()

	for {
		_, data, err := conn.Socket.ReadMessage()
		if err != nil {
			logger.Log.Warn("read error",
				zap.String("user", conn.UserID),
				zap.Error(err),
			)
			break
		}

		handleIncoming(conn, manager, data)
	}
}

func WriteLoop(conn *connection.Connection) {
	for msg := range conn.Send {
		err := conn.Socket.WriteMessage(websocket.TextMessage, msg)
		if err != nil {
			logger.Log.Warn("write error",
				zap.String("user", conn.UserID),
				zap.Error(err),
			)
			return
		}
	}
}

func handleIncoming(sender *connection.Connection, manager *connection.Manager, data []byte) {
	var msg message.Message
	if err := json.Unmarshal(data, &msg); err != nil {
		logger.Log.Error("invalid message",
			zap.Error(err),
		)
		return
	}

	if !redisClient.AllowMessage(sender.UserID, 5) {
		logger.Log.Warn("rate limited",
			zap.String("user", sender.UserID),
		)
		return
	}

	if msg.Type == "typing" {
		msg.From = sender.UserID
		msg.Origin = serverID

		out, _ := json.Marshal(msg)

		//publish to all servers (same as message flow)
		redisClient.Publish("typing", out)
		return
	}

	if msg.Type == "message" {
		metrics.MessagesTotal.Inc()

		msg.From = sender.UserID
		msg.Origin = serverID

		conversationID := msg.ConversationID
		msg.ConversationID = conversationID

		seq, err := redisClient.GetNextSequence(conversationID)
		if err != nil {
			return
		}
		msg.SequenceNumber = seq

		msgID := store.SaveMessage(msg.From, "", msg.Body, msg.ConversationID, msg.ClientMsgID, int64(msg.SequenceNumber))
		msg.MessageID = msgID

		out, _ := json.Marshal(msg)

		redisClient.Publish("chat", out)

		members := store.GetConversationMembers(msg.ConversationID)

		if len(members) == 0 {
			logger.Log.Warn("no members found",
				zap.String("conversation", msg.ConversationID),
			)
			return
		}

		for _, user := range members {
			if user == sender.UserID {
				continue
			}

			targetServers := redisClient.GetUserServers(user)
			for _, srv := range targetServers {
				if srv == serverID {
					targets := manager.Get(user)
					for _, conn := range targets {
						select {
						case conn.Send <- out:
						default:
							logger.Log.Warn("backpressure disconnect",
								zap.String("user", conn.UserID),
							)
							conn.Socket.Close()
							manager.Remove(conn.UserID, conn)
						}
					}
					continue
				}
				redisClient.Publish("chat:"+srv, out)

			}

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
		logger.Log.Error("upgrade failed", zap.Error(err))
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

	logger.Log.Info("user connected",
		zap.String("user", conn.UserID),
	)

	metrics.ActiveConnections.Inc()

	manager.Add(conn.UserID, conn)
	redisClient.AddUserServer(conn.UserID, serverID)

	go startHeartbeat(conn.UserID)

	msgs := store.GetUndelivered(conn.UserID)

	go WriteLoop(conn)
	go ReadLoop(conn)

	for _, m := range msgs {
		outMsg := message.Message{
			Type:           "message",
			From:           m.Sender,
			To:             m.Receiver,
			Body:           m.Body,
			MessageID:      m.ID,
			ConversationID: m.ConversationID,
			SequenceNumber: m.SequenceNumber,
			Origin:         "storage",
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

func getConversationID(a, b string) string {
	if a < b {
		return a + ":" + b
	}
	return b + ":" + a
}
