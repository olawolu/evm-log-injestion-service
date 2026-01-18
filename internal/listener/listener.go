// Package listener implements a webhook listener for evm log data
package listener

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/olawolu/evm-log-ingestion-service/internal/observability"
	"github.com/olawolu/evm-log-ingestion-service/internal/pipeline"
	"github.com/olawolu/evm-log-ingestion-service/internal/store"
)

var (
	name         = "usdc_transfers"
	filtervalues = []string{"0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"}
	network      = "polygon"
	// transformationCode = `function(block) { const USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174".toLowerCase(); const txfers = templates.tokenTransfers(block); return txfers.map((txfer, i) => ({ chain: block._network, block_number: txfer.blockNumber, transaction_hash: txfer.transactionHash, log_index: txfer.index || i, timestamp: txfer.timestamp, from_address: txfer.from, to_address: txfer.to, token_address: txfer.token, amount: txfer.amount })); }`
	transformationCode = `
	function (block) {
	  const USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174".toLowerCase(); // Polygon USDC
	  const txfers = templates.tokenTransfers(block);
	  return txfers
	    .filter(txfer => txfer.token?.toLowerCase() === USDC_ADDRESS)
	    .map((txfer, i) => ({
	      chain: block._network,
	      block_number: txfer.blockNumber,
	      transaction_hash: txfer.transactionHash,
	      log_index: txfer.index ?? i,
	      timestamp: txfer.timestamp,
	      from_address: txfer.from,
	      to_address: txfer.to,
	      token_address: txfer.token,
	      amount: txfer.amount
	    }));
	}`
)

type Listener struct {
	ctx           context.Context
	store         *store.ERC20TransferStore
	indexer       *pipeline.Indexer
	eventsChan    chan IndexedEvent // can be an external queue
	addressFilter *AddressFilter
}

func NewListener(
	ctx context.Context,
	store *store.ERC20TransferStore,
	indexer *pipeline.Indexer,
	addressFilter *AddressFilter,
	deliveryMechanism *pipeline.DeliveryMechanism,
	bufferSize int,
) (*Listener, error) {
	buffer := make(chan IndexedEvent, bufferSize)

	// httpHeaders := map[string]string{}
	// deliveryMechanism := pipeline.NewDeliveryMechanism("HTTP", "host", httpHeaders)

	indexerFilter, err := indexer.CreateFilter(name, filtervalues)
	if err != nil {
		err = fmt.Errorf("indexer error: %v", err)
		return nil, err
	}

	transformation, err := indexer.CreateTransformation(name, transformationCode)
	if err != nil {
		err = fmt.Errorf("indexer error: %v", err)
		return nil, err
	}

	err = indexer.CreatePipeline(name, transformation, indexerFilter, []string{"token_address"}, []string{network}, deliveryMechanism)
	if err != nil {
		err = fmt.Errorf("indexer error: %v", err)
		return nil, err
	}

	svc := &Listener{
		ctx:        ctx,
		store:      store,
		indexer:    indexer,
		eventsChan: buffer,
	}

	return svc, nil
}

func (l *Listener) StartWorkers(workers int) {
	for worker := range workers {
		go l.doWork(l.ctx, worker)
	}
}

func (l *Listener) EnqueueWork(events []IndexedEvent) error {
	for _, event := range events {
		select {
		case l.eventsChan <- event:
		default:
			return errors.New("listener queue full")
		}
	}
	return nil
}

func (l *Listener) DoBackfill(from, to uint64, filterValues []string) error {
	for _, value := range filtervalues {
		select {
		case <-l.ctx.Done():
			return l.ctx.Err()
		default:
			if err := l.indexer.BackfillHistorical(name, network, value, from, to); err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *Listener) doWork(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("worker %d shutting down", id)
			return
		case event := <-l.eventsChan:
			if err := l.processEvent(ctx, event); err != nil {
				observability.EventsFailed.Inc()
				log.Printf("worker %d failed to process event: %v", id, err)
				continue
			}

			observability.EventsProcessed.Inc()
			observability.QueueDepth.Set(float64(len(l.eventsChan)))
		}
	}
}

func (l *Listener) processEvent(ctx context.Context, event IndexedEvent) error {
	if !l.addressFilter.Match(event.FromAddress, event.ToAddress) {
		return nil
	}

	normalizedEvent := event.Normalize()
	if err := l.store.Insert(ctx, normalizedEvent); err != nil {
		return fmt.Errorf("error inserting event %v:%v in db: %v", normalizedEvent.TxHash, normalizedEvent.LogIndex, err)
	}
	return nil
}

// func handleEvents(events []IndexedEvent) {
// 	for _, e := range events {
// 		id, err := storeEvent(e)
// 		if err != nil {
// 			log.Println(err)
// 			return
// 		}
//
// 		jobs <- id
// 	}
// }
//==================================
//AddressFilter filters out addresses not in the defined filter

type AddressFilter struct {
	filter map[string]struct{} // set of strings
}

func NewAddressFilter(addresses []string) *AddressFilter {
	m := make(map[string]struct{}, len(addresses))
	for _, addr := range addresses {
		m[strings.ToLower(addr)] = struct{}{}
	}

	return &AddressFilter{filter: m}
}

func (af *AddressFilter) Match(from, to string) bool {
	_, okFrom := af.filter[strings.ToLower(from)]
	if okFrom {
		return true
	}

	_, okTo := af.filter[strings.ToLower(to)]
	return okTo
}

// '{"code":"function(block) { const txfers = templates.tokenTransfers(block); return txfers.map((txfer, i) => ({ chain: block._network, block_number: txfer.blockNumber, transaction_hash: txfer.transactionHash, log_index: txfer.index || i, timestamp: txfer.timestamp, from_address: txfer.from, to_address: txfer.to, token_address: txfer.token, amount: txfer.amount })); }"}'
