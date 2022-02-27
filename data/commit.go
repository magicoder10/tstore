package data

import (
	"time"
)

type Commit struct {
	CommittedTransactionID uint64    `json:"committed_transaction_id"`
	CommittedAt            time.Time `json:"committed_at"`
}
