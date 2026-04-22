package main

import (
	"log"
	"net/http"
	"distributed-chat-system/internal/server"
	"distributed-chat-system/internal/logger"
	"distributed-chat-system/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	logger.Init()
	defer logger.Log.Sync()

	metrics.Init()
	
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":2112",nil)
	}()
	
	http.HandleFunc("/ws", server.HandleWebSocket)
	http.HandleFunc("/create-group", server.CreateGroupHandler)

	log.Println("server running on :8080")
	http.ListenAndServe(":8080", nil)
}
