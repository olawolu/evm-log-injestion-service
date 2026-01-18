package store

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type ERC20TransferStore struct {
	collection *mongo.Collection
}

type ERC20Transfer struct {
	Chain       string    `bson:"chain"`
	BlockNumber uint64    `bson:"block_number"`
	TxHash      string    `bson:"tx_hash"`
	LogIndex    uint32    `bson:"log_index"`
	Timestamp   time.Time `bson:"timestamp"`

	FromAddress  string `bson:"from_address"`
	ToAddress    string `bson:"to_address"`
	TokenAddress string `bson:"token_address"`

	AmountRaw string `bson:"amount_raw"`
	Amount    int64  `bson:"amount"` // base units
}

type Direction string

const (
	DirectionIn   Direction = "in"
	DirectionOut  Direction = "out"
	DirectionBoth Direction = "both"
)

func NewERC20TransfersStore(db *mongo.Database) (*ERC20TransferStore, error) {
	coll := db.Collection("erc20_transfers")

	_, err := coll.Indexes().CreateMany(
		context.Background(),
		[]mongo.IndexModel{
			{
				Keys: bson.D{
					{Key: "transaction_hash", Value: 1},
					{Key: "log_index", Value: 1},
				},
				Options: options.Index().SetUnique(true),
			},
		},
	)
	if err != nil {
		return nil, err
	}

	return &ERC20TransferStore{collection: coll}, nil
}

func (s *ERC20TransferStore) Insert(ctx context.Context, t *ERC20Transfer) error {
	_, err := s.collection.InsertOne(ctx, t)
	return err
}

func (s *ERC20TransferStore) FindByAddressAndTimeRange(ctx context.Context, address string, from, to time.Time, direction Direction) ([]ERC20Transfer, error) {
	filter := bson.M{
		"timestamp": bson.M{
			"$gte": from,
			"$lte": to,
		},
	}

	switch direction {
	case DirectionIn:
		filter["to_address"] = address
	case DirectionOut:
		filter["from_address"] = address
	default:
		filter["$or"] = []bson.M{
			{"from_address": address},
			{"to_address": address},
		}
	}

	cur, err := s.collection.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var result []ERC20Transfer
	for cur.Next(ctx) {
		var t ERC20Transfer
		if err := cur.Decode(&t); err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	return result, nil
}
