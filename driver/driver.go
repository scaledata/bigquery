package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net/url"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

const (
	// accountIDParam is the DSN query parameter
	// name for the BigQuery job label account ID.
	accountIDParam = "account_id"

	// defaultAccountID is used when the account_id
	// parameter is not set in the DSN.
	defaultAccountID = "unspecified"
)

type BigQueryDriver struct {
}

type bigQueryConfig struct {
	projectID       string
	location        string
	dataSet         string
	scopes          []string
	endpoint        string
	disableAuth     bool
	credentialsFile string
	accountID  string
	// jobServerTimeout is the server-side timeout for
	// BQ jobs. It applies only to job execution time,
	// not queue/pending time. Set via the
	// "job_server_timeout" DSN query parameter (e.g.,
	// ?job_server_timeout=5m). BQ floors minimum
	// to 1s.
	jobServerTimeout time.Duration
}

func (b BigQueryDriver) Open(uri string) (driver.Conn, error) {

	if uri == "scanner" {
		return &scannerConnection{}, nil
	}

	config, err := configFromUri(uri)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	opts := []option.ClientOption{}
	if len(config.scopes) > 0 {
		opts = append(opts, option.WithScopes(config.scopes...))
	}
	if config.endpoint != "" {
		opts = append(opts, option.WithEndpoint(config.endpoint))
	}
	if config.disableAuth {
		opts = append(opts, option.WithoutAuthentication())
	}
	if config.credentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(config.credentialsFile))
	}

	client, err := bigquery.NewClient(ctx, config.projectID, opts...)
	if err != nil {
		return nil, err
	}

	return &bigQueryConnection{
		ctx:    ctx,
		client: client,
		config: *config,
	}, nil
}

func configFromUri(uri string) (*bigQueryConfig, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, invalidConnectionStringError(uri)
	}

	if u.Scheme != "bigquery" {
		return nil, fmt.Errorf("invalid prefix, expected bigquery:// got: %s", uri)
	}

	if u.Hostname() == "" {
		return nil, invalidConnectionStringError(uri)
	}

	fields := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(fields) > 2 {
		return nil, invalidConnectionStringError(uri)
	}

	// Check if dataset was provided
	datasetName := ""
	if len(fields) >= 1 {
		datasetName = fields[len(fields)-1]
	}

	accountID := u.Query().Get(accountIDParam)
	if accountID == "" {
		accountID = defaultAccountID
	}
	accountID = sanitizeLabelValue(accountID)

	config := &bigQueryConfig{
		projectID:       u.Hostname(),
		dataSet:         datasetName,
		scopes:          getScopes(u.Query()),
		endpoint:        u.Query().Get("endpoint"),
		disableAuth:     u.Query().Get("disable_auth") == "true",
		credentialsFile: u.Query().Get("credentials_file"),
		accountID:   accountID,
	}

	if v := u.Query().Get("job_server_timeout"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf(
				"bq driver: invalid job_server_timeout %q: %v",
				v, err,
			)
		}
		config.jobServerTimeout = d
	}

	if len(fields) == 2 {
		config.location = fields[0]
	}

	return config, nil
}

func getScopes(query url.Values) []string {
	q := strings.Trim(query.Get("scopes"), ",")
	if q == "" {
		return []string{}
	}
	return strings.Split(q, ",")
}

func invalidConnectionStringError(uri string) error {
	return fmt.Errorf("invalid connection string: %s", uri)
}

// sanitizeLabelValue converts a string into a valid GCP label value.
// GCP labels must contain only lowercase letters, digits, hyphens,
// and underscores, and be at most 63 characters.
func sanitizeLabelValue(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			b.WriteRune(c)
		} else {
			b.WriteRune('_')
		}
	}
	s = b.String()
	if len(s) > 63 {
		s = s[:63]
	}
	return s
}
