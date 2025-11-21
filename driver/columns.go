package driver

import (
	"database/sql/driver"
	"encoding/json"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"

	"github.com/scaledata/bigquery/adaptor"
)

type bigQuerySchema interface {
	ColumnNames() []string
	ConvertColumnValue(index int, value bigquery.Value) (driver.Value, error)
}

type bigQueryColumns struct {
	names   []string
	columns []bigQueryColumn
}

func (columns bigQueryColumns) ConvertColumnValue(index int, value bigquery.Value) (driver.Value, error) {
	if index > -1 && len(columns.columns) > index {
		column := columns.columns[index]
		return column.ConvertValue(value)
	}

	return value, nil
}

func (columns bigQueryColumns) ColumnNames() []string {
	return columns.names
}

type bigQueryReroutedColumn struct {
	values []bigquery.Value
	schema bigquery.Schema
}

func (c bigQueryReroutedColumn) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.values)
}

type bigQueryColumn struct {
	Name      string
	Schema    bigquery.Schema
	Adaptor   adaptor.SchemaColumnAdaptor
	FieldType bigquery.FieldType // Add field type information
}

func (column bigQueryColumn) ConvertValue(value bigquery.Value) (driver.Value, error) {
	// Handle DATE type conversion from civil.Date to time.Time
	if column.FieldType == bigquery.DateFieldType {
		if civilDate, ok := value.(civil.Date); ok {
			converted := time.Date(civilDate.Year, civilDate.Month, civilDate.Day, 0, 0, 0, 0, time.UTC)
			return converted, nil
		}
	}

	// Handle TIME type conversion from civil.Time to time.Time
	if column.FieldType == bigquery.TimeFieldType {
		if civilTime, ok := value.(civil.Time); ok {
			// Convert civil.Time to time.Time (today's date with the specified time in UTC)
			now := time.Now().UTC()
			converted := time.Date(now.Year(), now.Month(), now.Day(),
				civilTime.Hour, civilTime.Minute, civilTime.Second, civilTime.Nanosecond, time.UTC)
			return converted, nil
		}
	}

	// Handle DATETIME type conversion from civil.DateTime to time.Time
	if column.FieldType == bigquery.DateTimeFieldType {
		if civilDateTime, ok := value.(civil.DateTime); ok {
			converted := time.Date(civilDateTime.Date.Year, civilDateTime.Date.Month, civilDateTime.Date.Day,
				civilDateTime.Time.Hour, civilDateTime.Time.Minute, civilDateTime.Time.Second,
				civilDateTime.Time.Nanosecond, time.UTC)
			return converted, nil
		}
	}

	if len(column.Schema) == 0 {
		return value, nil
	}

	values, ok := value.([]bigquery.Value)
	if ok {

		if len(values) > 0 {
			if _, isRows := values[0].([]bigquery.Value); !isRows {
				values = []bigquery.Value{values}
			}
		}

		value = bigQueryReroutedColumn{values: values, schema: column.Schema}
	}

	if columnAdaptor := column.Adaptor; columnAdaptor != nil {
		return columnAdaptor.AdaptValue(value)
	}

	return value, nil
}

func createBigQuerySchema(schema bigquery.Schema, schemaAdaptor adaptor.SchemaAdaptor) bigQuerySchema {
	var names []string
	var columns []bigQueryColumn
	for _, column := range schema {

		name := column.Name

		var columnAdaptor adaptor.SchemaColumnAdaptor
		if schemaAdaptor != nil {
			columnAdaptor = schemaAdaptor.GetColumnAdaptor(name)
		}

		names = append(names, name)
		columns = append(columns, bigQueryColumn{
			Name:      name,
			Schema:    column.Schema,
			Adaptor:   columnAdaptor,
			FieldType: column.Type, // Pass the field type information
		})
	}
	return &bigQueryColumns{
		names,
		columns,
	}
}
