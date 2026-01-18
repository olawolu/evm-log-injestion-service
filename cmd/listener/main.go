package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"

	"github.com/joho/godotenv"
	"github.com/olawolu/evm-log-ingestion-service/internal/db"
	"github.com/olawolu/evm-log-ingestion-service/internal/listener"
	"github.com/olawolu/evm-log-ingestion-service/internal/observability"
	"github.com/olawolu/evm-log-ingestion-service/internal/pipeline"
	"github.com/olawolu/evm-log-ingestion-service/internal/store"
)

func main() {
	ctx := context.Background()
	if err := run(ctx, os.Getenv); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(
	ctx context.Context,
	getenv func(string) string,
) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()
	loadEnv()
	observability.Register()

	dbconn, err := db.ConnectDB(ctx, getenv("DB_URI"), getenv("DB_NAME"))
	if err != nil {
		return errors.New("failed to connect to database: " + err.Error())
	}

	store, err := store.NewERC20TransfersStore(dbconn.Database)
	if err != nil {
		return errors.New("failed to create store: " + err.Error())
	}

	addressFilter := listener.NewAddressFilter([]string{getenv("WALLET_ADDRESS")})
	indexer := pipeline.NewIndexer(getenv("INDEXER_API_KEY"), getenv("INDEXER_URL"))
	httpHeaders := map[string]string{
		"X-WEBHOOK-TOKEN": os.Getenv("INDEXER_WEBHOOK_TOKEN"),
	}

	deliveryMechanism := pipeline.NewDeliveryMechanism("HTTP", getenv("DELIVERY_HOST"), httpHeaders)

	listenerSvc, err := listener.NewListener(ctx, store, indexer, addressFilter, deliveryMechanism, 1000)
	if err != nil {
		return errors.New("failed to create listener: " + err.Error())
	}
	listenerSvc.StartWorkers(5)

	queryService := listener.NewQueryService(store)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("server listening on port %s\n", port)

	server := &http.Server{
		Addr:    ":" + port,
		Handler: listener.NewServer(listenerSvc, queryService),
	}
	go func() {
		log.Printf("listening on %s\n", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("error listening and serving: %s\n", err)
		}
	}()
	<-ctx.Done()
	server.Shutdown(context.Background())
	return nil
}

func loadEnv() {
	fmt.Println("loading environment...")
	godotenv.Load()
}
