package mutation

import (
	"fmt"
)

type SchemaNotFound string

func (s SchemaNotFound) Error() string {
	return fmt.Sprintf("schema not found: %v", (string)(s))
}

var _ error = (*SchemaNotFound)(nil)
