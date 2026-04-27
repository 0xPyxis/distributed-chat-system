package main

import (
	"distributed-chat-system/internal/logger"
	"distributed-chat-system/internal/metrics"
	"distributed-chat-system/internal/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
)

func main() {
	logger.Init()
	defer logger.Log.Sync()

	metrics.Init()

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":2112", nil)
	}()

	http.HandleFunc("/ws", server.HandleWebSocket)
	http.HandleFunc("/create-group", server.CreateGroupHandler)
	http.HandleFunc("/messages", server.GetMessagesHandler)
	http.HandleFunc("/seen", server.MarkSeenHandler)

	log.Println("server running on :8080")
	http.ListenAndServe(":8080", nil)
}
