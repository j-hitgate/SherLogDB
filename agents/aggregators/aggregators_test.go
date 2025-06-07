package aggregators

import (
	"math"
	"testing"

	sl "github.com/j-hitgate/sherlog"
	"github.com/stretchr/testify/assert"

	conds "main/agents/conditions"
	m "main/models"
	tt "main/test_tools"
)

func TestParseAggregator(t *testing.T) {
	tt.SherlogInit()
	trace := sl.NewTrace("Main")

	values := []any{7.0, 5.0}
	condition, _ := conds.ParseCondition(trace, "?0 > ?1", values, nil, nil)

	testCases := []struct {
		name   string
		s      string
		values []any
		aggr   m.IAggregator
		hasErr bool
	}{
		{
			name:   "none params",
			s:      "count[]",
			values: []any{},
			aggr:   &Count{},
		},
		{
			name:   "with condition",
			s:      "max[timestamp, ?0 > ?1]",
			values: values,
			aggr:   &Max{max: math.MinInt64, column: "timestamp", condition: condition},
		},
		{
			name:   "with condition",
			s:      "min[level]",
			values: []any{},
			aggr:   &Min{min: math.MaxInt64, column: "level"},
		},
		{
			name:   "with column",
			s:      "avg[level]",
			values: []any{},
			aggr:   &Avg{column: "level"},
		},
		{
			name:   "error: incorrect aggregator",
			s:      "abc[level]",
			values: []any{},
			hasErr: true,
		},
		{
			name:   "error: incorrect column type",
			s:      "max[message]",
			values: []any{},
			hasErr: true,
		},
		{
			name:   "error: too many args",
			s:      "max[level, a, b, c]",
			values: []any{},
			hasErr: true,
		},
		{
			name:   "error: invalid aggregator",
			s:      "count[",
			values: []any{},
			hasErr: true,
		},
	}

	for _, tc := range testCases {
		aggr, err := ParseAggregator(trace, tc.s, tc.values, nil)

		if err != nil {
			assert.True(t, tc.hasErr, tc.name, err.Error())
			continue
		}
		assert.False(t, tc.hasErr, tc.name)
		assert.True(t, tc.aggr.Equals(aggr), tc.name)
	}
}
