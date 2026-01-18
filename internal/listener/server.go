package listener

import (
	"fmt"
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func NewServer(listener *Listener, queryService *QueryService) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mux.Handle("POST /webhook", authMiddleware(WebhookHandler(listener)))
	mux.Handle("GET /query", QueryHandler(queryService))
	mux.Handle("POST /backfill", BackfillHandler(listener))
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

func authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("validating webhook")
		expectedToken := os.Getenv("INDEXER_WEBHOOK_TOKEN")
		token := r.Header.Get("X_WEBHOOK_TOKEN")
		fmt.Println("Token", token)
		if token == "" || token != expectedToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}
