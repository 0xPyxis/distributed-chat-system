package main

import (
	"log"
	"net/http"
	"distributed-chat-system/internal/server"
	"distributed-chat-system/internal/logger"
)

func main() {
	http.HandleFunc("/ws", server.HandleWebSocket)

	log.Println("server running on :8080")
	http.ListenAndServe(":8080", nil)

	logger.Init()
	defer logger.Log.Sync()
}
