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

type GetMessagesRequest struct {
	ConversationID string `json:"conversation_id"`
	BeforeSeq      int64  `json:"before_seq"`
	Limit          int    `json:"limit"`
}

func GetMessagesHandler(w http.ResponseWriter, r *http.Request) {
	var req GetMessagesRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", 400)
		return
	}

	// default values
	if req.Limit == 0 {
		req.Limit = 50
	}
	if req.BeforeSeq == 0 {
		req.BeforeSeq = 1 << 62 // very large number (start from latest)
	}

	msgs := store.GetMessages(req.ConversationID, req.BeforeSeq, req.Limit)

	resp, _ := json.Marshal(msgs)
	w.Write(resp)
}

type MarkSeenRequest struct {
	ConversationID string `json:"conversation_id"`
	SequenceNumber int64 `json:"sequence_number"`
	UserID 		   string `json:"user_id"`
}

func MarkSeenHandler(w http.ResponseWriter, r *http.Request){
	var req MarkSeenRequest
	if err:=json.NewDecoder(r.Body).Decode(&req); err!=nil{
		http.Error(w,"Invalid request",400)
		return
	}
	
	err:=store.UpdateLastSeen(req.ConversationID,req.UserID,req.SequenceNumber)
	if err!=nil{
		http.Error(w,"failed",500)
		return
	}
	w.WriteHeader(http.StatusOK)
}