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

type ColumnsTestSuite struct {
	suite.Suite
}

func TestColumnsTestSuite(t *testing.T) {
	suite.Run(t, new(ColumnsTestSuite))
}

// TestDateTime_ImplementsValuer is a compile-time check that
// DateTime satisfies the driver.Valuer interface.
func (s *ColumnsTestSuite) TestDateTime_ImplementsValuer() {
	var _ driver.Valuer = DateTime{}
}

func (s *ColumnsTestSuite) TestDateTimeValue() {
	testCases := map[string]struct {
		input    DateTime
		expected time.Time
	}{
		"basic_datetime": {
			input: DateTime{
				time.Date(2024, 6, 15, 10, 30, 45, 0, time.UTC),
			},
			expected: time.Date(2024, 6, 15, 10, 30, 45, 0, time.UTC),
		},
		"zero_time": {
			input:    DateTime{time.Time{}},
			expected: time.Time{},
		},
		"with_nanoseconds": {
			input: DateTime{
				time.Date(2024, 1, 1, 0, 0, 0, 123456000, time.UTC),
			},
			expected: time.Date(2024, 1, 1, 0, 0, 0, 123456000, time.UTC),
		},
	}

	for name, tc := range testCases {
		s.Run(name, func() {
			val, err := tc.input.Value()
			assert.NoError(s.T(), err)
			assert.Equal(s.T(), tc.expected, val)
		})
	}
}

func (s *ColumnsTestSuite) TestConvertValue() {
	testCases := map[string]struct {
		column       bigQueryColumn
		input        bigquery.Value
		expectedType string // "DateTime", "time.Time", "string", "nil"
		expected     interface{}
	}{
		"datetime_column": {
			column: bigQueryColumn{
				Name:      "created_at",
				FieldType: bigquery.DateTimeFieldType,
			},
			input: civil.DateTime{
				Date: civil.Date{Year: 2024, Month: 6, Day: 15},
				Time: civil.Time{Hour: 10, Minute: 30, Second: 45},
			},
			expectedType: "DateTime",
			expected: DateTime{
				time.Date(2024, 6, 15, 10, 30, 45, 0, time.UTC),
			},
		},
		"datetime_column_with_nanoseconds": {
			column: bigQueryColumn{
				Name:      "event_time",
				FieldType: bigquery.DateTimeFieldType,
			},
			input: civil.DateTime{
				Date: civil.Date{Year: 2024, Month: 1, Day: 1},
				Time: civil.Time{
					Hour: 12, Minute: 0, Second: 0,
					Nanosecond: 500000000,
				},
			},
			expectedType: "DateTime",
			expected: DateTime{
				time.Date(2024, 1, 1, 12, 0, 0, 500000000, time.UTC),
			},
		},
		"datetime_column_nil_value": {
			column: bigQueryColumn{
				Name:      "created_at",
				FieldType: bigquery.DateTimeFieldType,
			},
			input:        nil,
			expectedType: "nil",
			expected:     nil,
		},
		"date_column": {
			column: bigQueryColumn{
				Name:      "event_date",
				FieldType: bigquery.DateFieldType,
			},
			input:        civil.Date{Year: 2024, Month: 3, Day: 1},
			expectedType: "time.Time",
			expected:     time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
		},
		"time_column": {
			column: bigQueryColumn{
				Name:      "event_time",
				FieldType: bigquery.TimeFieldType,
			},
			input:        civil.Time{Hour: 14, Minute: 30, Second: 0},
			expectedType: "string",
			expected:     "14:30:00.000000",
		},
		"time_column_with_nanoseconds": {
			column: bigQueryColumn{
				Name:      "event_time",
				FieldType: bigquery.TimeFieldType,
			},
			input: civil.Time{
				Hour: 9, Minute: 5, Second: 3,
				Nanosecond: 123000000,
			},
			expectedType: "string",
			expected:     "09:05:03.123000",
		},
	}

	for name, tc := range testCases {
		s.Run(name, func() {
			result, err := tc.column.ConvertValue(tc.input)
			assert.NoError(s.T(), err)

			switch tc.expectedType {
			case "DateTime":
				dt, ok := result.(DateTime)
				assert.True(s.T(), ok, "expected DateTime, got %T", result)
				assert.Equal(s.T(), tc.expected, dt)
			case "time.Time":
				ts, ok := result.(time.Time)
				assert.True(s.T(), ok, "expected time.Time, got %T", result)
				assert.Equal(s.T(), tc.expected, ts)
			case "string":
				str, ok := result.(string)
				assert.True(s.T(), ok, "expected string, got %T", result)
				assert.Equal(s.T(), tc.expected, str)
			case "nil":
				assert.Nil(s.T(), result)
			}
		})
	}
}
