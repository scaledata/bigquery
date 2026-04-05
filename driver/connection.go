package driver

import (
	"context"
	"database/sql/driver"
	"fmt"

	"cloud.google.com/go/bigquery"
)

// DatasetProvider is an interface that provides access to the underlying
// BigQuery dataset. This interface can be used to access the BigQuery client
// for operations that are not supported by the standard database/sql interface,
// such as updating table clustering.
//
// Usage with database/sql:
//
//	conn, err := db.Conn(ctx)
//	if err != nil {
//	    return err
//	}
//	defer conn.Close()
//
//	err = conn.Raw(func(driverConn interface{}) error {
//	    if provider, ok := driverConn.(driver.DatasetProvider); ok {
//	        dataset := provider.GetDataset()
//	        // do operations on the dataset
//	    }
//	    return errors.New("connection does not implement DatasetProvider")
//	})
type DatasetProvider interface {
	// GetDataset returns the BigQuery dataset for the current connection.
	GetDataset() *bigquery.Dataset
}

type bigQueryConnection struct {
	ctx     context.Context
	client  *bigquery.Client
	config  bigQueryConfig
	closed  bool
	bad     bool
	dataset *bigquery.Dataset
}

// Ensure bigQueryConnection implements DatasetProvider
var _ DatasetProvider = (*bigQueryConnection)(nil)

func (connection *bigQueryConnection) GetDataset() *bigquery.Dataset {
	if connection.dataset != nil {
		return connection.dataset
	}
	connection.dataset = connection.client.Dataset(connection.config.dataSet)
	return connection.dataset
}

func (connection *bigQueryConnection) GetContext() context.Context {
	return connection.ctx
}

func (connection *bigQueryConnection) Ping(ctx context.Context) error {

	dataset := connection.GetDataset()
	if dataset == nil {
		return fmt.Errorf("faild to ping using '%s' dataset", connection.config.dataSet)
	}

	_, err := dataset.Metadata(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (connection *bigQueryConnection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	var statement = &bigQueryStatement{connection, query}
	return statement.QueryContext(ctx, args)
}

func (connection *bigQueryConnection) Query(query string, args []driver.Value) (driver.Rows, error) {
	statement, err := connection.Prepare(query)
	if err != nil {
		return nil, nil
	}

	return statement.Query(args)
}

func (connection *bigQueryConnection) Prepare(query string) (driver.Stmt, error) {
	var statement = &bigQueryStatement{connection, query}

	return statement, nil
}

func (connection *bigQueryConnection) Close() error {
	if connection.closed {
		return nil
	}
	if connection.bad {
		return driver.ErrBadConn
	}
	connection.closed = true
	return connection.client.Close()
}

func (connection *bigQueryConnection) Begin() (driver.Tx, error) {
	var transaction = &bigQueryTransaction{connection}

	return transaction, nil
}

func (connection *bigQueryConnection) query(query string) (*bigquery.Query, error) {
	return connection.client.Query(query), nil
}

func (connection *bigQueryConnection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	var statement = &bigQueryStatement{connection, query}
	return statement.ExecContext(ctx, args)
}

func (connection *bigQueryConnection) Exec(query string, args []driver.Value) (driver.Result, error) {
	var statement = &bigQueryStatement{connection, query}
	return statement.Exec(args)
}

func (bigQueryConnection) CheckNamedValue(*driver.NamedValue) error {
	// TODO: Revise in the future
	return nil
}
