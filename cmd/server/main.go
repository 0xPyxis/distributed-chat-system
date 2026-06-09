package main

import (
	"distributed-chat-system/internal/logger"
	"distributed-chat-system/internal/metrics"
	"distributed-chat-system/internal/server"
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	logger.Init()
	defer logger.Log.Sync()

	metrics.Init()

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(":2112", nil); err != nil {
			log.Fatalf("metrics server failed: %v", err)
		}
	}()

	http.HandleFunc("/ws", server.HandleWebSocket)
	http.HandleFunc("/create-group", server.CreateGroupHandler)
	http.HandleFunc("/messages", server.GetMessagesHandler)
	http.HandleFunc("/seen", server.MarkSeenHandler)

	log.Println("server running on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
