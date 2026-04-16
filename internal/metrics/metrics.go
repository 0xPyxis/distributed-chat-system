package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	MessagesTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "messages_total",
			Help: "Total messages processed",
		},
	)

	ActiveConnections = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "active_connections",
			Help: "Current active connections",
		},
	)
)

func Init() {
	prometheus.MustRegister(MessagesTotal)
	prometheus.MustRegister(ActiveConnections)
}