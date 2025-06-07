package log_utils

import (
	"testing"

	sl "github.com/j-hitgate/sherlog"
	"github.com/stretchr/testify/assert"

	m "main/models"
	tt "main/test_tools"
)

func TestGetRange(t *testing.T) {
	tt.SherlogInit()
	trace := sl.NewTrace("Main")
	ls := &Selector{}

	logs := []*m.Log{
		{Timestamp: 1},
		{Timestamp: 2},
		{Timestamp: 3},
		{Timestamp: 4},
		{Timestamp: 5},
		{Timestamp: 6},
	}

	testCases := []struct {
		name     string
		tr       m.TimeRange
		startInx int
		endInx   int
		logsLen  int
	}{
		{
			name:     "empty range",
			startInx: 0,
			endInx:   len(logs),
			logsLen:  len(logs),
		},
		{
			name:     "start and end in range",
			tr:       m.TimeRange{Start: 2, End: 4},
			startInx: 1,
			endInx:   4,
			logsLen:  3,
		},
		{
			name:     "start in range",
			tr:       m.TimeRange{Start: 3},
			startInx: 2,
			endInx:   len(logs),
			logsLen:  len(logs) - 2,
		},
		{
			name:     "end in range",
			tr:       m.TimeRange{End: 4},
			startInx: 0,
			endInx:   4,
			logsLen:  len(logs) - 2,
		},
		{
			name:     "start out of range",
			tr:       m.TimeRange{Start: -1},
			startInx: 0,
			endInx:   len(logs),
			logsLen:  len(logs),
		},
		{
			name:     "end out of range",
			tr:       m.TimeRange{End: 8},
			startInx: 0,
			endInx:   len(logs),
			logsLen:  len(logs),
		},
		{
			name:     "not crossed",
			tr:       m.TimeRange{Start: 8},
			startInx: -1,
			endInx:   -1,
			logsLen:  0,
		},
	}

	for _, ts := range testCases {
		startInx, endInx := ls.GetIndicesOfRange(trace, logs, ts.tr, true)
		assert.Equal(t, ts.startInx, startInx, ts.name)
		assert.Equal(t, ts.endInx, endInx, ts.name)

		logs_ := ls.GetLogsInRange(trace, logs, ts.tr, true)
		assert.Equal(t, ts.logsLen, len(logs_), ts.name)

		logs_ = ls.GetLogsOutOfRange(trace, logs, ts.tr, true)
		assert.Equal(t, len(logs)-ts.logsLen, len(logs_), ts.name)
	}
}
