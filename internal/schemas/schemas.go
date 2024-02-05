package schemas

import "github.com/apache/arrow/go/v14/arrow"

type Table struct {
	Name   string        `json:"name"`
	Schema *arrow.Schema `json:"schema"`
}
