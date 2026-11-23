package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"distributed-chat-system/internal/connection"
	"distributed-chat-system/internal/logger"
	"distributed-chat-system/internal/message"

	"github.com/alicebob/miniredis/v2"
	"github.com/gorilla/websocket"
)

func TestEndToEndChat(t *testing.T) {
	logger.Init()

	s := miniredis.NewMiniRedis()
	if err := s.StartAddr("127.0.0.1:6379"); err != nil {
		t.Logf("MiniRedis: %v", err)
	} else {
		defer s.Close()
	}

	convID := fmt.Sprintf("test_conv_t_%d", time.Now().UnixNano())
	if err := store.CreateConversation(convID, []string{"user_a", "user_b"}); err != nil {
		t.Fatalf("CreateConversation failed: %v", err)
	}

	ts := httptest.NewServer(http.HandlerFunc(HandleWebSocket))
	defer ts.Close()

	u, _ := url.Parse(ts.URL)
	u.Scheme = "ws"

	// Connect User A
	wsA, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		t.Fatalf("Dial user_a failed: %v", err)
	}
	defer wsA.Close()
	wsA.WriteJSON(message.Message{Type: "auth", From: "user_a"})

	// Connect User B
	wsB, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		t.Fatalf("Dial user_b failed: %v", err)
	}
	defer wsB.Close()
	wsB.WriteJSON(message.Message{Type: "auth", From: "user_b"})

	time.Sleep(200 * time.Millisecond)

	// User A sends message to convID
	clientMsgID := fmt.Sprintf("client_msg_%d", time.Now().UnixNano())
	msg := message.Message{
		Type:           "message",
		From:           "user_a",
		ConversationID: convID,
		ClientMsgID:    clientMsgID,
		Body:           "Hello User B from Test!",
	}
	if err := wsA.WriteJSON(msg); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// User B receives message
	wsB.SetReadDeadline(time.Now().Add(2 * time.Second))
	var received message.Message
	if err := wsB.ReadJSON(&received); err != nil {
		t.Fatalf("User B failed to receive: %v", err)
	}

	if received.Body != "Hello User B from Test!" {
		t.Errorf("Unexpected body: %q", received.Body)
	}
	if received.From != "user_a" {
		t.Errorf("Unexpected sender: %q", received.From)
	}
	if received.SequenceNumber <= 0 {
		t.Errorf("Expected positive sequence number, got %d", received.SequenceNumber)
	}

	// User B sends ACK
	wsB.WriteJSON(message.Message{
		Type:      "ack",
		From:      "user_b",
		MessageID: received.MessageID,
	})

	time.Sleep(200 * time.Millisecond)

	// Verify database marked delivered
	var delivered bool
	store.DB.QueryRow("SELECT delivered FROM messages WHERE id = $1", received.MessageID).Scan(&delivered)
	if !delivered {
		t.Errorf("Expected message %d to be marked delivered=true in DB", received.MessageID)
	}
}

func TestCrossServerPubSubDelivery(t *testing.T) {
	logger.Init()

	s := miniredis.NewMiniRedis()
	if err := s.StartAddr("127.0.0.1:6379"); err != nil {
		t.Logf("MiniRedis: %v", err)
	} else {
		defer s.Close()
	}

	convID := fmt.Sprintf("test_cross_conv_%d", time.Now().UnixNano())
	if err := store.CreateConversation(convID, []string{"alice", "bob"}); err != nil {
		t.Fatalf("CreateConversation failed: %v", err)
	}

	// Mock server-2 manager with connected Bob
	server2Manager := connection.NewManager()
	bobChan := make(chan []byte, 10)
	connBob := &connection.Connection{
		UserID: "bob",
		Send:   bobChan,
	}
	server2Manager.Add("bob", connBob)
	redisClient.AddUserServer("bob", "server-2")

	// Server-2 subscribes using the updated subscriber logic
	redisClient.Subscribe("chat:server-2", func(data []byte) {
		var msg message.Message
		json.Unmarshal(data, &msg)

		if msg.To != "" {
			for _, conn := range server2Manager.Get(msg.To) {
				conn.Send <- data
			}
		} else if msg.ConversationID != "" {
			members := store.GetConversationMembers(msg.ConversationID)
			for _, user := range members {
				if user == msg.From {
					continue
				}
				for _, conn := range server2Manager.Get(user) {
					conn.Send <- data
				}
			}
		}
	})

	time.Sleep(200 * time.Millisecond)

	// Deliver message from server-1
	testMsg := message.Message{
		Type:           "message",
		From:           "alice",
		ConversationID: convID,
		Body:           "Cross server test payload",
		SequenceNumber: 99,
	}
	payload, _ := json.Marshal(testMsg)
	deliverMessage(&testMsg, payload)

	select {
	case receivedBytes := <-bobChan:
		var received message.Message
		json.Unmarshal(receivedBytes, &received)
		if received.Body != "Cross server test payload" {
			t.Errorf("Expected 'Cross server test payload', got %q", received.Body)
		}
		if received.From != "alice" {
			t.Errorf("Expected sender 'alice', got %q", received.From)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for Bob to receive cross-server message")
	}
}
