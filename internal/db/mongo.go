package db

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type DB struct {
	Client   *mongo.Client
	Database *mongo.Database
}

func ConnectDB(ctx context.Context, uri, dbName string) (*DB, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	clientOpts := options.Client().ApplyURI(uri).
		SetServerSelectionTimeout(5 * time.Second).
		SetRetryWrites(true).
		SetMaxPoolSize(50)

	client, err := mongo.Connect(clientOpts)
	if err != nil {
		return nil, err
	}

	if err = client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return nil, err
	}

	return &DB{
		Client:   client,
		Database: client.Database(dbName),
	}, nil
}
