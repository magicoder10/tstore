package mutation

import (
	"fmt"
)

type SchemaNotFound string

func (s SchemaNotFound) Error() string {
	return fmt.Sprintf("schema not found: %v", s)
}

var _ error = (*SchemaNotFound)(nil)
