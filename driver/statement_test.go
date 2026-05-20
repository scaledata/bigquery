package driver

import (
	"database/sql/driver"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type StatementTestSuite struct {
	suite.Suite
}

func TestStatementTestSuite(t *testing.T) {
	suite.Run(t, new(StatementTestSuite))
}

func (s *StatementTestSuite) TestBuildParameter() {
	ts := time.Date(2024, 6, 15, 10, 30, 45, 0, time.UTC)

	testCases := map[string]struct {
		arg           driver.Value
		existing      []bigquery.QueryParameter
		expectedLen   int
		expectedValue interface{}
		expectedName  string
	}{
		"datetime_converts_to_civil": {
			arg:           DateTime{ts},
			expectedLen:   1,
			expectedValue: civil.DateTimeOf(ts),
		},
		"regular_string_passes_through": {
			arg:           "hello",
			expectedLen:   1,
			expectedValue: "hello",
		},
		"regular_int64_passes_through": {
			arg:           int64(42),
			expectedLen:   1,
			expectedValue: int64(42),
		},
		"named_value_with_name": {
			arg: driver.NamedValue{
				Name:  "param1",
				Value: int64(42),
			},
			expectedLen:   1,
			expectedValue: int64(42),
			expectedName:  "param1",
		},
		"accumulates_with_existing": {
			arg:           "second",
			existing:      []bigquery.QueryParameter{{Value: "first"}},
			expectedLen:   2,
			expectedValue: "second",
		},
	}

	for name, tc := range testCases {
		s.Run(name, func() {
			params := buildParameter(tc.arg, tc.existing)
			assert.Equal(s.T(), tc.expectedLen, len(params))

			last := params[len(params)-1]
			assert.Equal(s.T(), tc.expectedValue, last.Value)
			if tc.expectedName != "" {
				assert.Equal(s.T(), tc.expectedName, last.Name)
			}
		})
	}
}

func (s *StatementTestSuite) TestBuildParameterFromNamedValue() {
	ts := time.Date(2024, 6, 15, 10, 30, 45, 0, time.UTC)

	testCases := map[string]struct {
		namedValue    driver.NamedValue
		expectedValue interface{}
		expectedName  string
	}{
		"datetime_named_converts_to_civil": {
			namedValue: driver.NamedValue{
				Name:  "created_at",
				Value: DateTime{ts},
			},
			expectedValue: civil.DateTimeOf(ts),
			expectedName:  "created_at",
		},
		"datetime_unnamed_converts_to_civil": {
			namedValue: driver.NamedValue{
				Value: DateTime{ts},
			},
			expectedValue: civil.DateTimeOf(ts),
		},
		"regular_value_named": {
			namedValue: driver.NamedValue{
				Name:  "count",
				Value: int64(99),
			},
			expectedValue: int64(99),
			expectedName:  "count",
		},
		"regular_value_unnamed": {
			namedValue: driver.NamedValue{
				Value: "hello",
			},
			expectedValue: "hello",
		},
	}

	for name, tc := range testCases {
		s.Run(name, func() {
			params := buildParameterFromNamedValue(
				tc.namedValue, nil,
			)
			assert.Equal(s.T(), 1, len(params))
			assert.Equal(s.T(), tc.expectedValue, params[0].Value)
			assert.Equal(s.T(), tc.expectedName, params[0].Name)
		})
	}
}
