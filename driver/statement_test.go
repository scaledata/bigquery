package driver

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeBigQuery serves the BigQuery job endpoints the driver calls. Every
// inserted job is RUNNING on insert and reports jobStatus on jobs.get.
// jobs.getQueryResults answers with resultsStatus and an error carrying
// resultsReason when resultsStatus is not 200, the way BigQuery reports a
// job that failed on a quota.
type fakeBigQuery struct {
	jobStatus     map[string]any
	resultsStatus int
	resultsReason string
	statusCalls   int32
	resultsCalls  int32
}

func (f *fakeBigQuery) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/jobs"):
		var job map[string]any
		_ = json.NewDecoder(r.Body).Decode(&job)
		job["status"] = map[string]any{"state": "RUNNING"}
		_ = json.NewEncoder(w).Encode(job)
	case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/queries/"):
		atomic.AddInt32(&f.resultsCalls, 1)
		if f.resultsStatus != http.StatusOK {
			w.WriteHeader(f.resultsStatus)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
				"code":    f.resultsStatus,
				"message": "Job exceeded rate limits",
				"errors": []map[string]any{{
					"reason":  f.resultsReason,
					"message": "Job exceeded rate limits",
				}},
			}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jobComplete": true,
			"totalRows":   "0",
			"schema":      map[string]any{"fields": []any{}},
			"rows":        []any{},
		})
	case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/jobs/"):
		calls := atomic.AddInt32(&f.statusCalls, 1)
		status := f.jobStatus
		if calls == 1 {
			status = map[string]any{"state": "RUNNING"}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jobReference": map[string]any{"projectId": "proj", "jobId": "job"},
			"status":       status,
		})
	case strings.HasSuffix(r.URL.Path, "/cancel"):
		_ = json.NewEncoder(w).Encode(map[string]any{})
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func execWithFake(
	t *testing.T,
	fake *fakeBigQuery,
	params string,
	timeout time.Duration,
) error {
	t.Helper()
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	dsn := "bigquery://proj/ds?disable_auth=true&endpoint=" +
		url.QueryEscape(server.URL+"/") + params
	conn, err := BigQueryDriver{}.Open(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err = conn.(driver.ExecerContext).ExecContext(
		ctx, "ALTER TABLE t ADD COLUMN c INT64", nil,
	)
	return err
}

var quotaFailedJob = map[string]any{
	"state": "DONE",
	"errorResult": map[string]any{
		"reason":   "rateLimitExceeded",
		"location": "table.write",
		"message":  "Exceeded rate limits: too many table update operations for this table.",
	},
}

func TestExecContextQuotaFailedJob(t *testing.T) {
	t.Run("poll_job_status returns the job's error at once", func(t *testing.T) {
		fake := &fakeBigQuery{
			jobStatus:     quotaFailedJob,
			resultsStatus: http.StatusBadRequest,
			resultsReason: "jobRateLimitExceeded",
		}
		start := time.Now()
		err := execWithFake(t, fake, "&poll_job_status=true", 30*time.Second)

		require.Error(t, err)
		var jobErr *bigquery.Error
		require.ErrorAs(t, err, &jobErr)
		assert.Equal(t, "rateLimitExceeded", jobErr.Reason)
		assert.Contains(t, jobErr.Message, "too many table update operations")
		assert.Less(t, time.Since(start), 5*time.Second)
		assert.Zero(t, atomic.LoadInt32(&fake.resultsCalls))
	})

	t.Run("without poll_job_status the error waits for the deadline", func(t *testing.T) {
		fake := &fakeBigQuery{
			jobStatus:     quotaFailedJob,
			resultsStatus: http.StatusBadRequest,
			resultsReason: "jobRateLimitExceeded",
		}
		timeout := 2 * time.Second
		start := time.Now()
		err := execWithFake(t, fake, "", timeout)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
		assert.GreaterOrEqual(t, time.Since(start), timeout)
		assert.Greater(t, atomic.LoadInt32(&fake.resultsCalls), int32(1))
	})
}

func TestExecContextPollJobStatusSucceeds(t *testing.T) {
	fake := &fakeBigQuery{
		jobStatus:     map[string]any{"state": "DONE"},
		resultsStatus: http.StatusOK,
	}
	err := execWithFake(t, fake, "&poll_job_status=true", 30*time.Second)

	require.NoError(t, err)
	assert.Equal(t, int32(2), atomic.LoadInt32(&fake.statusCalls))
	assert.Equal(t, int32(1), atomic.LoadInt32(&fake.resultsCalls))
}
