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
	"sync"
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
var store = storage.NewStore()

// ✅ GLOBAL (FIXED — was wrong before)
var pendingAcks sync.Map // messageID -> time

func init() {

	// ================= CHAT SUBSCRIBE =================
	redisClient.Subscribe("chat:"+serverID, func(data []byte) {
		var msg message.Message
		if err := json.Unmarshal(data, &msg); err != nil {
			logger.Log.Error("unmarshal failed", zap.Error(err))
			return
		}

		targets := manager.Get(msg.To)

		for _, conn := range targets {
			select {
			case conn.Send <- data:
			default:
				conn.Socket.Close()
				manager.Remove(conn.UserID, conn)
			}
		}
	})

	// ================= TYPING =================
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

	// ================= RETRY LOOP =================
	go retryLoop()
}

// ================= RETRY LOOP =================
func retryLoop() {
	for {
		time.Sleep(5 * time.Second)

		pendingAcks.Range(func(key, value interface{}) bool {
			msgID := key.(int)
			sentTime := value.(time.Time)

			if time.Since(sentTime) > 5*time.Second {
				logger.Log.Warn("retrying message", zap.Int("msgID", msgID))

				msg := store.GetMessageByID(msgID)
				if msg == nil {
					return true
				}

				out, _ := json.Marshal(msg)

				// resend using fanout
				deliverMessage(msg, out)

				pendingAcks.Store(msgID, time.Now())
			}
			return true
		})
	}
}

// ================= CORE DELIVERY =================
func deliverMessage(msg *message.Message, out []byte) {

	members := store.GetConversationMembers(msg.ConversationID)

	// ✅ FANOUT OPTIMIZATION
	serverMap := make(map[string][]string)

	for _, user := range members {
		if user == msg.From {
			continue
		}

		targetServers := redisClient.GetUserServers(user)

		for _, srv := range targetServers {
			serverMap[srv] = append(serverMap[srv], user)
		}
	}

	for srv, users := range serverMap {

		if srv == serverID {
			for _, user := range users {
				targets := manager.Get(user)

				for _, conn := range targets {
					select {
					case conn.Send <- out:
					default:
						conn.Socket.Close()
						manager.Remove(conn.UserID, conn)
					}
				}
			}
			continue
		}

		redisClient.Publish("chat:"+srv, out)
	}
}

// ================= HANDLE INCOMING =================
func handleIncoming(sender *connection.Connection, manager *connection.Manager, data []byte) {

	var msg message.Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return
	}

	if !redisClient.AllowMessage(sender.UserID, 5) {
		return
	}

	// ===== TYPING =====
	if msg.Type == "typing" {
		msg.From = sender.UserID
		msg.Origin = serverID

		out, _ := json.Marshal(msg)
		redisClient.Publish("typing", out)
		return
	}

	// ===== MESSAGE =====
	if msg.Type == "message" {

		metrics.MessagesTotal.Inc()

		msg.From = sender.UserID
		msg.Origin = serverID

		seq, err := redisClient.GetNextSequence(msg.ConversationID)
		if err != nil {
			return
		}
		msg.SequenceNumber = seq

		msgID := store.SaveMessage(
			msg.From,
			"",
			msg.Body,
			msg.ConversationID,
			msg.ClientMsgID,
			msg.SequenceNumber,
		)

		msg.MessageID = msgID

		out, _ := json.Marshal(msg)

		// store pending ACK
		pendingAcks.Store(msgID, time.Now())

		deliverMessage(&msg, out)
	}

	// ===== ACK =====
	if msg.Type == "ack" {
		store.MarkDelivered(msg.MessageID)
		pendingAcks.Delete(msg.MessageID)
	}
}

// ================= SOCKET HANDLING =================
func ReadLoop(conn *connection.Connection) {
	defer func() {
		metrics.ActiveConnections.Dec()
		conn.Socket.Close()
		manager.Remove(conn.UserID, conn)
		close(conn.Send)
		redisClient.RemoveUserServer(conn.UserID, serverID)
	}()

	for {
		_, data, err := conn.Socket.ReadMessage()
		if err != nil {
			break
		}

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
			Body:           m.Body,
			MessageID:      m.ID,
			ConversationID: m.ConversationID,
			SequenceNumber: m.SequenceNumber,
		}
		data, _ := json.Marshal(outMsg)
		conn.Send <- data
	}
}

func startHeartbeat(userID string) {
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		redisClient.AddUserServer(userID, serverID)
	}
}