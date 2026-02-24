package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	LabelsProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "label_consumer_labels_processed_total",
			Help: "Total number of labels processed",
		},
		[]string{"labeler_host", "operation"},
	)

	Connected = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "label_consumer_connected",
			Help: "Whether the consumer is connected to the labeler (1=connected, 0=disconnected)",
		},
		[]string{"labeler_host"},
	)

	CursorValue = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "label_consumer_cursor_value",
			Help: "Current cursor value for each labeler",
		},
		[]string{"labeler_host"},
	)
)

func init() {
	prometheus.MustRegister(LabelsProcessed, Connected, CursorValue)
}
