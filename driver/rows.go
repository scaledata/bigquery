package driver

import (
	"database/sql/driver"
	"io"

	"cloud.google.com/go/bigquery"

	"github.com/scaledata/bigquery/adaptor"
	"google.golang.org/api/iterator"
)

type bigQueryRows struct {
	source  bigQuerySource
	schema  bigQuerySchema
	adaptor adaptor.SchemaAdaptor
}

func (rows *bigQueryRows) ensureSchema() {
	if rows.schema == nil {
		rows.schema = rows.source.GetSchema()
	}
}

func (rows *bigQueryRows) Columns() []string {
	rows.ensureSchema()
	return rows.schema.ColumnNames()
}

func (rows *bigQueryRows) Close() error {
	return nil
}

// SourceRowIterator returns the underlying BigQuery
// RowIterator if the source is backed by one.
// Returns nil otherwise.
func (rows *bigQueryRows) SourceRowIterator() *bigquery.RowIterator {
	src, ok :=
		rows.source.(*bigQueryRowIteratorSource)
	if !ok {
		return nil
	}
	return src.iterator
}

func (rows *bigQueryRows) Next(dest []driver.Value) error {

	rows.ensureSchema()

	values, err := rows.source.Next()
	if err == iterator.Done {
		return io.EOF
	}

	if err != nil {
		return err
	}

	var length = len(values)
	for i := range dest {
		if i < length {
			dest[i], err = rows.schema.ConvertColumnValue(i, values[i])
			if err != nil {
				return err
			}
		}
	}

	return nil
}
