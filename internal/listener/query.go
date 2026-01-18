package listener

import (
	"context"
	"time"

	"github.com/olawolu/evm-log-ingestion-service/internal/store"
)

type QueryService struct {
	store *store.ERC20TransferStore
}

func NewQueryService(store *store.ERC20TransferStore) *QueryService {
	return &QueryService{store: store}
}

func (s *QueryService) GetTransfers(
	ctx context.Context,
	address string,
	from, to time.Time,
	direction store.Direction,
) ([]store.ERC20Transfer, error) {
	return s.store.FindByAddressAndTimeRange(
		ctx,
		address,
		from,
		to,
		direction,
	)
}
