package driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/sirupsen/logrus"

	"github.com/scaledata/bigquery/adaptor"
)

const (
	// jobCancelTimeout bounds the goroutine that
	// sends a cancellation request to the BQ API.
	jobCancelTimeout = 5 * time.Minute
)

type bigQueryStatement struct {
	connection *bigQueryConnection
	query      string
}

func (statement bigQueryStatement) Close() error {
	return nil
}

func (statement bigQueryStatement) NumInput() int {
	return 0
}

func (bigQueryStatement) CheckNamedValue(*driver.NamedValue) error {
	return nil
}

func (statement *bigQueryStatement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	logrus.Debugf("exec:%s", statement.query)

	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		for _, arg := range args {
			logrus.Debugf("- param:%s", convertParameterToValue(arg))
		}
	}

	query, err := statement.buildQuery(ctx, convertParameters(args))
	if err != nil {
		return nil, err
	}

	rowIterator, err := runQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	return &bigQueryResult{rowIterator}, nil
}

func (statement *bigQueryStatement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {

	logrus.Debugf("query:%s", statement.query)

	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		for _, arg := range args {
			logrus.Debugf("- param:%s", convertParameterToValue(arg))
		}
	}

	if statement.query == adaptor.RerouteQuery {

		if len(args) < 1 {
			return nil, errors.New("expected a rerouting argument")
		}

		column, ok := args[0].Value.(bigQueryReroutedColumn)
		if !ok {
			return nil, errors.New("expected a rerouting argument with rows")
		}

		schemaAdaptor := adaptor.GetSchemaAdaptor(ctx)
		if schemaAdaptor == nil {
			return nil, errors.New("expected a rerouting schema adaptor")
		}

		schema := createBigQuerySchema(column.schema, schemaAdaptor)

		return &bigQueryRows{
			source: createSourceFromColumn(schema, column.values),
		}, nil
	}

	query, err := statement.buildQuery(ctx, convertParameters(args))
	if err != nil {
		return nil, err
	}

	rowIterator, err := runQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	return &bigQueryRows{
		source: createSourceFromRowIterator(rowIterator, adaptor.GetSchemaAdaptor(ctx)),
	}, nil

}

func (statement bigQueryStatement) Exec(args []driver.Value) (driver.Result, error) {

	logrus.Debugf("exec:%s", statement.query)

	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		for _, arg := range args {
			logrus.Debugf("- param:%s", convertParameterToValue(arg))
		}
	}

	query, err := statement.buildQuery(context.Background(), args)
	if err != nil {
		return nil, err
	}

	rowIterator, err := query.Read(context.Background())
	if err != nil {
		return nil, err
	}

	return &bigQueryResult{rowIterator}, nil
}

func (statement bigQueryStatement) Query(args []driver.Value) (driver.Rows, error) {

	logrus.Debugf("query:%s", statement.query)
	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		for _, arg := range args {
			logrus.Debugf("- param:%s", convertParameterToValue(arg))
		}
	}

	query, err := statement.buildQuery(context.Background(), args)
	if err != nil {
		return nil, err
	}

	rowIterator, err := query.Read(context.Background())
	if err != nil {
		return nil, err
	}

	return &bigQueryRows{source: createSourceFromRowIterator(rowIterator, nil)}, nil
}

func (statement bigQueryStatement) buildQuery(ctx context.Context, args []driver.Value) (*bigquery.Query, error) {

	query, err := statement.connection.query(statement.query)
	if err != nil {
		return nil, err
	}
	query.DefaultDatasetID = statement.connection.config.dataSet
	query.JobTimeout = effectiveJobTimeout(
		ctx,
		statement.connection.config.jobServerTimeout,
	)

	query.Parameters, err = statement.buildParameters(args)
	if err != nil {
		return nil, err
	}

	if statement.connection.config.reservation != "" {
		query.Reservation = statement.connection.config.reservation
	}

	return query, err
}

// effectiveJobTimeout returns the smaller of the
// DSN-configured server timeout and the caller's
// context deadline. If neither is set, returns 0
// (no timeout).
func effectiveJobTimeout(
	ctx context.Context,
	configured time.Duration,
) time.Duration {
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining > 0 &&
			(configured == 0 || remaining < configured) {
			return remaining
		}
	}
	return configured
}

// runQuery submits a BQ query via Run and blocks
// until results are ready via Read. If Read fails
// and the caller's context is done, the job is
// cancelled server-side.
func runQuery(
	ctx context.Context,
	query *bigquery.Query,
) (*bigquery.RowIterator, error) {
	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}
	rowIterator, err := job.Read(ctx)
	if err != nil {
		cancelJobIfContextDone(ctx, job)
		return nil, err
	}
	return rowIterator, nil
}

// cancelJobIfContextDone fires an async BQ job
// cancellation when the caller's context is done
// (cancelled or deadline exceeded). The goroutine is
// bounded by jobCancelTimeout.
func cancelJobIfContextDone(
	ctx context.Context,
	job *bigquery.Job,
) {
	if ctx.Err() == nil {
		return
	}
	go func() {
		cancelCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			jobCancelTimeout,
		)
		defer cancel()
		if err := job.Cancel(cancelCtx); err != nil {
			logrus.Warnf(
				"bq driver: failed to cancel job: %v",
				err,
			)
		}
	}()
}

func (statement bigQueryStatement) buildParameters(args []driver.Value) ([]bigquery.QueryParameter, error) {
	if args == nil {
		return nil, nil
	}

	var parameters []bigquery.QueryParameter
	for _, arg := range args {
		parameters = buildParameter(arg, parameters)
	}
	return parameters, nil
}

func buildParameter(arg driver.Value, parameters []bigquery.QueryParameter) []bigquery.QueryParameter {
	namedValue, ok := arg.(driver.NamedValue)
	if ok {
		return buildParameterFromNamedValue(namedValue, parameters)
	}

	logrus.Debugf("-param:%s", arg)

	return append(parameters, bigquery.QueryParameter{
		Value: arg,
	})
}

func buildParameterFromNamedValue(namedValue driver.NamedValue, parameters []bigquery.QueryParameter) []bigquery.QueryParameter {
	logrus.Debugf("-param:%s=%s", namedValue.Name, namedValue.Value)

	if namedValue.Name == "" {
		return append(parameters, bigquery.QueryParameter{
			Value: namedValue.Value,
		})
	} else {
		return append(parameters, bigquery.QueryParameter{
			Name:  namedValue.Name,
			Value: namedValue.Value,
		})
	}
}

func convertParameters(args []driver.NamedValue) []driver.Value {
	var values []driver.Value
	if args != nil {
		for _, arg := range args {
			values = append(values, arg)
		}
	}
	return values
}
func convertParameterToValue(value driver.Value) interface{} {
	namedValue, ok := value.(driver.NamedValue)
	if ok {
		return namedValue.Value
	}
	return value
}
