package mutation

import (
	"fmt"

	"tstore/data"
)

type LogLine interface {
	Line() string
}

type TransactionStartLogLine struct {
	TransactionID uint64
}

func (t TransactionStartLogLine) Line() string {
	return fmt.Sprintf("start %v\n", t.TransactionID)
}

var _ LogLine = (*TransactionStartLogLine)(nil)

type TransactionCommittedLogLine struct {
	TransactionID uint64
}

func (t TransactionCommittedLogLine) Line() string {
	return fmt.Sprintf("committed %v\n", t.TransactionID)
}

var _ LogLine = (*TransactionCommittedLogLine)(nil)

type TransactionAbortedLogLine struct {
	TransactionID uint64
}

func (t TransactionAbortedLogLine) Line() string {
	return fmt.Sprintf("aborted %v\n", t.TransactionID)
}

var _ LogLine = (*TransactionAbortedLogLine)(nil)

type TransactionCreateSchemaLogLine struct {
	TransactionID uint64
	MutationType  data.MutationType
	SchemaName    string
}
