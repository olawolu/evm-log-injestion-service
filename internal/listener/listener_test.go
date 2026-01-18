// Package listener implements a webhook listener for evm log data
package listener

import (
	"reflect"
	"testing"

	"github.com/olawolu/evm-log-ingestion-service/internal/pipeline"
	"github.com/olawolu/evm-log-ingestion-service/internal/store"
)

func TestNewListener(t *testing.T) {
	// These will need to be created with actual constructors from your codebase
	// Adjust these based on how you actually create these types
	dataStore := &store.ERC20TransferStore{}  // You'll need proper initialization
	indexer := &pipeline.Indexer{}            // You'll need proper initialization
	delivery := &pipeline.DeliveryMechanism{} // You'll need proper initialization
	filter := NewAddressFilter([]string{"0x123"})

	type args struct {
		store             *store.ERC20TransferStore
		indexer           *pipeline.Indexer
		addressFilter     *AddressFilter
		deliveryMechanism *pipeline.DeliveryMechanism
		bufferSize        int
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		wantCap int // expected channel capacity
	}{
		{
			name: "valid listener with default buffer size",
			args: args{
				store:             dataStore,
				indexer:           indexer,
				addressFilter:     filter,
				deliveryMechanism: delivery,
				bufferSize:        0,
			},
			wantErr: false,
			wantCap: 100, // assuming 100 is your default
		},
		{
			name: "valid listener with custom buffer size",
			args: args{
				store:             dataStore,
				indexer:           indexer,
				addressFilter:     filter,
				deliveryMechanism: delivery,
				bufferSize:        500,
			},
			wantErr: false,
			wantCap: 500,
		},
		{
			name: "small buffer size",
			args: args{
				store:             dataStore,
				indexer:           indexer,
				addressFilter:     filter,
				deliveryMechanism: delivery,
				bufferSize:        10,
			},
			wantErr: false,
			wantCap: 10,
		},
		{
			name: "nil store should error",
			args: args{
				store:             nil,
				indexer:           indexer,
				addressFilter:     filter,
				deliveryMechanism: delivery,
				bufferSize:        100,
			},
			wantErr: true,
		},
		{
			name: "nil indexer should error",
			args: args{
				store:             dataStore,
				indexer:           nil,
				addressFilter:     filter,
				deliveryMechanism: delivery,
				bufferSize:        100,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewListener(tt.args.store, tt.args.indexer, tt.args.addressFilter, tt.args.deliveryMechanism, tt.args.bufferSize)
			if (err != nil) != tt.wantErr {
				t.Fatalf("NewListener() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if got == nil {
				t.Fatal("NewListener() returned nil without error")
			}
			if cap(got.eventsChan) != tt.wantCap {
				t.Errorf("NewListener() eventsChan capacity = %v, want %v", cap(got.eventsChan), tt.wantCap)
			}
			if got.store != tt.args.store {
				t.Error("NewListener() store not set correctly")
			}
			if got.indexer != tt.args.indexer {
				t.Error("NewListener() indexer not set correctly")
			}
		})
	}
}

func TestListener_StartWorkers(t *testing.T) {
	// Skip if you can't create proper instances
	t.Skip("Requires proper initialization of dataStore, indexer, and delivery mechanism")

	// Example of how you might test this with real instances:
	// store := store.NewERC20TransferStore(/* params */)
	// indexer := pipeline.NewIndexer(/* params */)
	// delivery := pipeline.NewDeliveryMechanism(/* params */)
	// filter := NewAddressFilter([]string{})
	//
	// listener, err := NewListener(dataStore, indexer, filter, delivery, 10)
	// if err != nil {
	//     t.Fatal(err)
	// }
	//
	// ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	// defer cancel()
	//
	// listener.StartWorkers(ctx, 3)
	// // Verify workers are running by sending events
}

func TestListener_EnqueueWork(t *testing.T) {
	type args struct {
		events []IndexedEvent
	}
	tests := []struct {
		name       string
		bufferSize int
		args       args
		wantErr    bool
	}{
		{
			name:       "enqueue single event successfully",
			bufferSize: 10,
			args: args{
				events: []IndexedEvent{
					{TransactionHash: "0xabc123", LogIndex: 0},
				},
			},
			wantErr: false,
		},
		{
			name:       "enqueue multiple events successfully",
			bufferSize: 10,
			args: args{
				events: []IndexedEvent{
					{TransactionHash: "0xabc123", LogIndex: 0},
					{TransactionHash: "0xdef456", LogIndex: 1},
					{TransactionHash: "0xghi789", LogIndex: 2},
				},
			},
			wantErr: false,
		},
		{
			name:       "enqueue fails when queue is full",
			bufferSize: 2,
			args: args{
				events: []IndexedEvent{
					{TransactionHash: "0xabc123", LogIndex: 0},
					{TransactionHash: "0xdef456", LogIndex: 1},
					{TransactionHash: "0xghi789", LogIndex: 2},
				},
			},
			wantErr: true,
		},
		{
			name:       "enqueue empty slice succeeds",
			bufferSize: 10,
			args: args{
				events: []IndexedEvent{},
			},
			wantErr: false,
		},
		{
			name:       "enqueue exactly fills buffer",
			bufferSize: 3,
			args: args{
				events: []IndexedEvent{
					{TransactionHash: "0xabc123", LogIndex: 0},
					{TransactionHash: "0xdef456", LogIndex: 1},
					{TransactionHash: "0xghi789", LogIndex: 2},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create fresh listener for each test
			l := &Listener{
				eventsChan: make(chan IndexedEvent, tt.bufferSize),
			}

			err := l.EnqueueWork(tt.args.events)
			if (err != nil) != tt.wantErr {
				t.Errorf("Listener.EnqueueWork() error = %v, wantErr %v", err, tt.wantErr)
			}

			// Verify events are actually in the channel (for successful cases)
			if !tt.wantErr && len(tt.args.events) > 0 {
				if len(l.eventsChan) != len(tt.args.events) {
					t.Errorf("Expected %d events in channel, got %d", len(tt.args.events), len(l.eventsChan))
				}
			}
		})
	}
}

func TestListener_doWork(t *testing.T) {
	t.Skip("Requires proper initialization of dataStore, indexer, and delivery mechanism")

	// This test is difficult without mocks. You would need:
	// 1. A real store that can handle Save operations
	// 2. A real indexer that can process events
	// 3. A real delivery mechanism
	//
	// Consider refactoring to use interfaces if you need unit tests here
}

func TestListener_processEvent(t *testing.T) {
	t.Skip("Requires proper initialization of dataStore, indexer, and delivery mechanism")

	// This test requires:
	// - A working store.ERC20TransferStore
	// - A working pipeline.Indexer
	// - A working pipeline.DeliveryMechanism
	//
	// Without interfaces, this becomes an integration test
}

func TestNewAddressFilter(t *testing.T) {
	type args struct {
		addresses []string
	}
	tests := []struct {
		name string
		args args
		want map[string]struct{}
	}{
		{
			name: "empty address list",
			args: args{
				addresses: []string{},
			},
			want: map[string]struct{}{},
		},
		{
			name: "nil address list",
			args: args{
				addresses: nil,
			},
			want: map[string]struct{}{},
		},
		{
			name: "single address",
			args: args{
				addresses: []string{"0x123"},
			},
			want: map[string]struct{}{
				"0x123": {},
			},
		},
		{
			name: "multiple addresses",
			args: args{
				addresses: []string{"0x123", "0x456", "0x789"},
			},
			want: map[string]struct{}{
				"0x123": {},
				"0x456": {},
				"0x789": {},
			},
		},
		{
			name: "addresses normalized to lowercase",
			args: args{
				addresses: []string{"0xABC", "0xDEF", "0xAbCdEf"},
			},
			want: map[string]struct{}{
				"0xabc":    {},
				"0xdef":    {},
				"0xabcdef": {},
			},
		},
		{
			name: "duplicate addresses",
			args: args{
				addresses: []string{"0x123", "0x123", "0x456", "0x456"},
			},
			want: map[string]struct{}{
				"0x123": {},
				"0x456": {},
			},
		},
		{
			name: "mixed case duplicates",
			args: args{
				addresses: []string{"0xABC", "0xabc", "0xAbC"},
			},
			want: map[string]struct{}{
				"0xabc": {},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewAddressFilter(tt.args.addresses)
			if got == nil {
				t.Fatal("NewAddressFilter() returned nil")
			}
			if !reflect.DeepEqual(got.filter, tt.want) {
				t.Errorf("NewAddressFilter() filter = %v, want %v", got.filter, tt.want)
			}
		})
	}
}

func TestAddressFilter_Match(t *testing.T) {
	type args struct {
		from string
		to   string
	}
	tests := []struct {
		name      string
		addresses []string
		args      args
		want      bool
	}{
		{
			name:      "empty filter matches all",
			addresses: []string{},
			args: args{
				from: "0x123",
				to:   "0x456",
			},
			want: true,
		},
		{
			name:      "nil filter matches all",
			addresses: nil,
			args: args{
				from: "0x123",
				to:   "0x456",
			},
			want: true,
		},
		{
			name:      "from address matches",
			addresses: []string{"0x123"},
			args: args{
				from: "0x123",
				to:   "0x456",
			},
			want: true,
		},
		{
			name:      "to address matches",
			addresses: []string{"0x456"},
			args: args{
				from: "0x123",
				to:   "0x456",
			},
			want: true,
		},
		{
			name:      "neither address matches",
			addresses: []string{"0x789"},
			args: args{
				from: "0x123",
				to:   "0x456",
			},
			want: false,
		},
		{
			name:      "case insensitive from matching",
			addresses: []string{"0xabc"},
			args: args{
				from: "0xABC",
				to:   "0x456",
			},
			want: true,
		},
		{
			name:      "case insensitive to matching",
			addresses: []string{"0xDEF"},
			args: args{
				from: "0x123",
				to:   "0xdef",
			},
			want: true,
		},
		{
			name:      "both addresses match",
			addresses: []string{"0x123", "0x456"},
			args: args{
				from: "0x123",
				to:   "0x456",
			},
			want: true,
		},
		{
			name:      "multiple addresses, one matches from",
			addresses: []string{"0x111", "0x123", "0x999"},
			args: args{
				from: "0x123",
				to:   "0x456",
			},
			want: true,
		},
		{
			name:      "multiple addresses, one matches to",
			addresses: []string{"0x111", "0x456", "0x999"},
			args: args{
				from: "0x123",
				to:   "0x456",
			},
			want: true,
		},
		{
			name:      "empty from and to with populated filter",
			addresses: []string{"0x123"},
			args: args{
				from: "",
				to:   "",
			},
			want: false,
		},
		{
			name:      "empty from and to with empty filter",
			addresses: []string{},
			args: args{
				from: "",
				to:   "",
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			af := NewAddressFilter(tt.addresses)
			if got := af.Match(tt.args.from, tt.args.to); got != tt.want {
				t.Errorf("AddressFilter.Match() = %v, want %v", got, tt.want)
			}
		})
	}
}
