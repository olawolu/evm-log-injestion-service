package listener

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/olawolu/evm-log-ingestion-service/internal/observability"
	"github.com/olawolu/evm-log-ingestion-service/internal/store"
)

type IndexedEvent struct {
	Chain           string `json:"chain,omitempty"`
	BlockNumber     int64  `json:"block_number,omitempty"`
	TransactionHash string `json:"transaction_hash,omitempty"`
	LogIndex        int    `json:"log_index,omitempty"`
	Timestamp       string `json:"timestamp,omitempty"`
	FromAddress     string `json:"from_address,omitempty"`
	ToAddress       string `json:"to_address,omitempty"`
	TokenAddress    string `json:"token_address,omitempty"`
	Amount          string `json:"amount,omitempty"`
}

func (e *IndexedEvent) Normalize() *store.ERC20Transfer {
	amount, err := strconv.ParseInt(e.Amount, 10, 64)
	if err != nil {
		log.Println(err)
	}

	timeStamp, err := time.Parse(time.RFC3339, e.Timestamp)
	if err != nil {
		log.Println(err)
	}

	return &store.ERC20Transfer{
		Chain:        e.Chain,
		BlockNumber:  uint64(e.BlockNumber),
		TxHash:       e.TransactionHash,
		LogIndex:     uint32(e.LogIndex),
		Timestamp:    timeStamp,
		FromAddress:  e.FromAddress,
		ToAddress:    e.ToAddress,
		TokenAddress: e.TokenAddress,
		AmountRaw:    e.Amount,
		Amount:       amount,
	}
}

func NewWebhookHandler(listener *Listener) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observability.WebhookRequests.WithLabelValues("received").Inc()
		decodedEvents, err := decodeJSON[[]IndexedEvent](r)
		if err != nil {
			log.Println(err)
			return
		}

		fmt.Println("decodedEvents", decodedEvents)

		err = listener.EnqueueWork(decodedEvents)
		if err != nil {
			log.Println(err)
		}

		observability.WebhookRequests.WithLabelValues("accepted").Inc()
		w.WriteHeader(http.StatusAccepted)
	})
}

func NewQueryHandler(svc *QueryService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		q := r.URL.Query()
		address := q.Get("address")
		if address == "" {
			http.Error(w, "address is required", http.StatusBadRequest)
			return
		}

		from := parseTime(q.Get("from"), time.Time{})
		to := parseTime(q.Get("to"), time.Now().UTC())

		direction := store.Direction(q.Get("direction"))
		if direction == "" {
			direction = store.DirectionBoth
		}

		res, err := svc.GetTransfers(ctx, address, from, to, direction)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		err = encodeJSON(w, http.StatusOK, res)
		if err != nil {
			log.Println(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
}

func parseTime(v string, def time.Time) time.Time {
	if v == "" {
		return def
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return def
	}
	return t
}

func decodeJSON[T any](r *http.Request) (T, error) {
	var v T
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		return v, fmt.Errorf("decode json: %w", err)
	}
	return v, nil
}

func encodeJSON[T any](w http.ResponseWriter, status int, v T) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		return fmt.Errorf("encode json: %w", err)
	}
	return nil
}
