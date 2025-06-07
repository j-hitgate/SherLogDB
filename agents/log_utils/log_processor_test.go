package log_utils

import (
	"fmt"
	"testing"

	sl "github.com/j-hitgate/sherlog"
	"github.com/stretchr/testify/assert"

	m "main/models"
	tt "main/test_tools"
	"main/tools"
)

func TestProcessor(t *testing.T) {
	logs := []*m.Log{
		{
			Entity:    "log1",
			Traces:    []string{"trace1"},
			Level:     0,
			Modules:   []string{"m1", "m2"},
			Timestamp: 1,
		},
		{
			Entity:    "log2",
			Traces:    []string{"trace1"},
			Level:     1,
			Modules:   []string{"m2", "m3"},
			Timestamp: 2,
		},
		{
			Entity:    "log3",
			Traces:    []string{"trace2"},
			Level:     2,
			Modules:   []string{"m3", "m4"},
			Timestamp: 3,
		},
		{
			Entity:    "log4",
			Traces:    []string{"trace2"},
			Level:     3,
			Modules:   []string{"m4", "m5"},
			Timestamp: 4,
		},
	}

	testCases := []struct {
		name     string
		query    *m.SearchQuery
		logs     []*m.Log
		lld      *m.LoadLogsData
		result   [][]any
		hasError bool
	}{
		{
			name: "simple query",
			query: &m.SearchQuery{
				Storage:     "storage",
				Select:      []string{"entity", "modules"},
				Where:       "level >= ?0 & level != ?1",
				WhereValues: []any{1.0, 2.0},
				OrderBy:     "entity",
			},
			logs: logs,
			lld: &m.LoadLogsData{
				Storage: "storage",
				Columns: map[string]bool{
					"entity":  true,
					"level":   true,
					"modules": true,
				},
			},
			result: [][]any{
				{"log2", []string{"m2", "m3"}},
				{"log4", []string{"m4", "m5"}},
			},
		},
		{
			name: "aggregation query",
			query: &m.SearchQuery{
				Storage:      "storage",
				Select:       []string{"traces", "sum[level, level > ?0]"},
				AggregValues: []any{1.0},
				GroupBy:      "traces",
				Having:       "count[] > ?0",
				HavingValues: []any{0.0},
				OrderBy:      "traces",
			},
			logs: logs,
			lld: &m.LoadLogsData{
				Storage: "storage",
				Columns: map[string]bool{
					"traces": true,
					"level":  true,
				},
			},
			result: [][]any{
				{[]string{"trace1"}, int64(0)},
				{[]string{"trace2"}, int64(5)},
			},
		},
	}

	for _, tc := range testCases {
		// Create processor

		tt.SherlogInit()
		proc, lld, err := NewProcessor(sl.NewTrace("Main"), tc.query)

		if err != nil {
			assert.True(t, tc.hasError, fmt.Sprintf("%s: %s", tc.name, err.Error()))
			return
		}
		assert.False(t, tc.hasError, tc.name)

		// Put logs

		err = proc.PutLogs(tc.logs[:2])

		if err != nil {
			assert.True(t, tc.hasError, fmt.Sprintf("%s: %s", tc.name, err.Error()))
			return
		}
		assert.False(t, tc.hasError, tc.name)

		err = proc.PutLogs(tc.logs[2:])

		if err != nil {
			assert.True(t, tc.hasError, fmt.Sprintf("%s: %s", tc.name, err.Error()))
			return
		}
		assert.False(t, tc.hasError, tc.name)

		// Get results

		result, err := proc.GetResult()

		if err != nil {
			assert.True(t, tc.hasError, fmt.Sprintf("%s: %s", tc.name, err.Error()))
			return
		}
		assert.False(t, tc.hasError, tc.name)

		// Check results

		assert.True(t, tools.EqualSlicesBy(tc.result, result, func(arr1, arr2 []any) bool {
			return tools.EqualSlicesBy(arr1, arr2, func(val1, val2 any) bool {
				switch val1.(type) {
				case int64, string:
					return val1 == val2
				case []string:
					return tools.EqualSlices(val1.([]string), val2.([]string))
				case []int64:
					return tools.EqualSlices(val1.([]int64), val2.([]int64))
				default:
					return false
				}
			})
		}), tc.name)

		assert.True(t, tc.lld.Equals(lld), tc.name)
	}
}
