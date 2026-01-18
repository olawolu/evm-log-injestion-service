package observability

import "github.com/prometheus/client_golang/prometheus"

var (
	WebhookRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "webhook_requests_total",
			Help: "Total webhook requests received",
		},
		[]string{"status"},
	)

	EventsProcessed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "events_processed_total",
			Help: "Total events processed",
		},
	)

	EventsFailed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "events_failed_total",
			Help: "Total failed events",
		},
	)

	QueueDepth = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "events_queue_depth",
			Help: "Current event queue depth",
		},
	)
)

func Register() {
	prometheus.MustRegister(
		WebhookRequests,
		EventsProcessed,
		QueueDepth,
	)
}
