package schemas

import "github.com/apache/arrow/go/v15/arrow"

type DataTableSchema struct {
	TableName string
	Schema    *arrow.Schema
}
