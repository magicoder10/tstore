package mutation

import (
	"fmt"
	"strings"

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
	MutationType  Type
	SchemaName    string
}

func (t TransactionCreateSchemaLogLine) Line() string {
	return fmt.Sprintf(
		"mutate %v %v %v\n",
		t.TransactionID,
		t.MutationType,
		t.SchemaName)
}

var _ LogLine = (*TransactionCreateSchemaLogLine)(nil)

type TransactionDeleteSchemaLogLine struct {
	TransactionID  uint64
	MutationType   Type
	SchemaName     string
	PrevAttributes map[string]data.Type
}

func (t TransactionDeleteSchemaLogLine) Line() string {
	return fmt.Sprintf(
		"mutate %v %v %v %v\n",
		t.TransactionID,
		t.MutationType,
		t.SchemaName,
		mapToString(t.PrevAttributes))
}

var _ LogLine = (*TransactionDeleteSchemaLogLine)(nil)

type TransactionCreateSchemaAttributesLogLine struct {
	TransactionID     uint64
	MutationType      Type
	SchemaName        string
	CreatedAttributes []string
}

func (t TransactionCreateSchemaAttributesLogLine) Line() string {
	return fmt.Sprintf(
		"mutate %v %v %v %v\n",
		t.TransactionID,
		t.MutationType,
		t.SchemaName,
		sliceToString(t.CreatedAttributes))
}

var _ LogLine = (*TransactionCreateSchemaAttributesLogLine)(nil)

type TransactionDeleteSchemaAttributesLogLine struct {
	TransactionID  uint64
	MutationType   Type
	SchemaName     string
	PrevAttributes map[string]data.Type
}

func (t TransactionDeleteSchemaAttributesLogLine) Line() string {
	return fmt.Sprintf(
		"mutate %v %v %v %v\n",
		t.TransactionID,
		t.MutationType,
		t.SchemaName,
		mapToString(t.PrevAttributes))
}

var _ LogLine = (*TransactionDeleteSchemaAttributesLogLine)(nil)

type TransactionCreateEntityLogLine struct {
	TransactionID uint64
	MutationType  Type
	EntityID      uint64
}

func (t TransactionCreateEntityLogLine) Line() string {
	return fmt.Sprintf(
		"mutate %v %v %v\n",
		t.TransactionID,
		t.MutationType,
		t.EntityID)
}

var _ LogLine = (*TransactionCreateEntityLogLine)(nil)

type TransactionDeleteEntityLogLine struct {
	TransactionID  uint64
	MutationType   Type
	EntityID       uint64
	PrevAttributes map[string]interface{}
}

func (t TransactionDeleteEntityLogLine) Line() string {
	return fmt.Sprintf(
		"mutate %v %v %v %v\n",
		t.TransactionID,
		t.MutationType,
		t.EntityID,
		mapToString(t.PrevAttributes))
}

var _ LogLine = (*TransactionDeleteEntityLogLine)(nil)

type TransactionCreateEntityAttributesLogLine struct {
	TransactionID     uint64
	MutationType      Type
	EntityID          uint64
	CreatedAttributes []string
}

func (t TransactionCreateEntityAttributesLogLine) Line() string {
	return fmt.Sprintf(
		"mutate %v %v %v %v\n",
		t.TransactionID,
		t.MutationType,
		t.EntityID,
		sliceToString(t.CreatedAttributes))
}

var _ LogLine = (*TransactionCreateEntityAttributesLogLine)(nil)

type TransactionDeleteEntityAttributesLogLine struct {
	TransactionID  uint64
	MutationType   Type
	EntityID       uint64
	PrevAttributes map[string]interface{}
}

func (t TransactionDeleteEntityAttributesLogLine) Line() string {
	return fmt.Sprintf(
		"mutate %v %v %v %v\n",
		t.TransactionID,
		t.MutationType,
		t.EntityID,
		mapToString(t.PrevAttributes))
}

var _ LogLine = (*TransactionDeleteEntityAttributesLogLine)(nil)

type TransactionUpdateEntityAttributesLogLine struct {
	TransactionID  uint64
	MutationType   Type
	EntityID       uint64
	PrevAttributes map[string]interface{}
}

func (t TransactionUpdateEntityAttributesLogLine) Line() string {
	return fmt.Sprintf(
		"mutate %v %v %v %v\n",
		t.TransactionID,
		t.MutationType,
		t.EntityID,
		mapToString(t.PrevAttributes))
}

var _ LogLine = (*TransactionUpdateEntityAttributesLogLine)(nil)

func mapToString[Key data.Comparable, Value any](input map[Key]Value) string {
	var pairs []string
	for key, value := range input {
		// TODO: encode value to remove special character
		pairs = append(pairs, fmt.Sprintf("%v=%v", key, value))
	}

	return fmt.Sprintf("{%s}", strings.Join(pairs, ","))
}

func sliceToString[Item any](input []Item) string {
	var pairs []string
	for _, value := range input {
		// TODO: encode value to remove special character
		pairs = append(pairs, fmt.Sprintf("%v", value))
	}

	return fmt.Sprintf("[%s]", strings.Join(pairs, ","))
}
