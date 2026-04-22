package server

import (
	"distributed-chat-system/internal/logger"
	"encoding/json"
	"go.uber.org/zap"
	"net/http"
)

type CreateGroupRequest struct {
	ConversationID string   `json:"conversation_id"`
	Members        []string `json:"members"`
}

func CreateGroupHandler(w http.ResponseWriter, r *http.Request) {
	var req CreateGroupRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", 400)
		return
	}

	err := store.CreateConversation(req.ConversationID, req.Members)
	if err != nil {
		logger.Log.Error("create group failed", zap.Error(err))
		http.Error(w, "failed", 500)
		return
	}

	w.WriteHeader(http.StatusOK)
}
