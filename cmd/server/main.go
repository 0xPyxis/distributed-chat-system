package main

import (
	"log"
	"net/http"
	"distributed-chat-system/internal/server"
)

func main() {
	http.HandleFunc("/ws", server.HandleWebSocket)

	log.Println("server running on :8080")
	http.ListenAndServe(":8080", nil)
}
