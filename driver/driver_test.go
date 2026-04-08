package driver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigFromUri(t *testing.T) {
	tests := []struct {
		name        string
		uri         string
		expected    *bigQueryConfig
		expectError bool
	}{
		{
			name: "basic URI without reservation",
			uri:  "bigquery://my-project/US/my-dataset",
			expected: &bigQueryConfig{
				projectID:   "my-project",
				location:    "US",
				dataSet:     "my-dataset",
				scopes:      []string{},
				reservation: "",
			},
		},
		{
			name: "URI with reservation",
			uri:  "bigquery://my-project/US/my-dataset?reservation=projects/my-project/locations/US/reservations/my-reservation",
			expected: &bigQueryConfig{
				projectID:   "my-project",
				location:    "US",
				dataSet:     "my-dataset",
				scopes:      []string{},
				reservation: "projects/my-project/locations/US/reservations/my-reservation",
			},
		},
		{
			name: "URI with reservation and other params",
			uri:  "bigquery://my-project/US/my-dataset?endpoint=http://localhost:9050&disable_auth=true&reservation=projects/p/locations/US/reservations/bg",
			expected: &bigQueryConfig{
				projectID:   "my-project",
				location:    "US",
				dataSet:     "my-dataset",
				scopes:      []string{},
				endpoint:    "http://localhost:9050",
				disableAuth: true,
				reservation: "projects/p/locations/US/reservations/bg",
			},
		},
		{
			name:        "invalid scheme",
			uri:         "mysql://my-project/US/my-dataset",
			expectError: true,
		},
		{
			name:        "missing host",
			uri:         "bigquery:///",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := configFromUri(tt.uri)
			if tt.expectError {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected.projectID, config.projectID)
			assert.Equal(t, tt.expected.location, config.location)
			assert.Equal(t, tt.expected.dataSet, config.dataSet)
			assert.Equal(t, tt.expected.scopes, config.scopes)
			assert.Equal(t, tt.expected.endpoint, config.endpoint)
			assert.Equal(t, tt.expected.disableAuth, config.disableAuth)
			assert.Equal(t, tt.expected.reservation, config.reservation)
		})
	}
}
