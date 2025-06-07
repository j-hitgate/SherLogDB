package log_utils

import (
	"sort"

	sl "github.com/j-hitgate/sherlog"

	"main/agents/time_range"
	m "main/models"
	"main/tools"
)

type Selector struct{}

func (*Selector) GetIndicesOfRange(trace *sl.Trace, logs []*m.Log, tr m.TimeRange, isSorted bool) (startInx, endInx int) {
	defer trace.AddModule("_StorageAgent", "GetIndicesOfRange")()

	if tr.Start == 0 && tr.End == 0 {
		trace.DEBUG(nil, "All logs passed")
		return 0, len(logs)
	}

	if len(logs) == 0 {
		return -1, -1
	}

	if !isSorted {
		sort.Slice(logs, func(i, j int) bool {
			return logs[i].Timestamp < logs[j].Timestamp
		})
	}

	// If all logs out of range

	lastLog := logs[len(logs)-1]

	logsTr := m.TimeRange{
		Start: logs[0].Timestamp,
		End:   lastLog.Timestamp,
	}

	if !time_range.IsCrossed(tr, logsTr) {
		return -1, -1
	}

	startInx, endInx = 0, len(logs)

	// Find start index

	if tr.Start != 0 && logs[0].Timestamp < tr.Start {
		startInx = sort.Search(len(logs), func(i int) bool {
			return tr.Start <= logs[i].Timestamp
		})
	}

	// Find end index

	if tr.End != 0 && tr.End < lastLog.Timestamp {
		endInx = sort.Search(len(logs), func(i int) bool {
			return tr.End < logs[i].Timestamp
		})
	}

	trace.DEBUG(nil, endInx-startInx, "/", len(logs), " logs passed")
	return startInx, endInx
}

func (s *Selector) GetLogsInRange(trace *sl.Trace, logs []*m.Log, tr m.TimeRange, isSorted bool) []*m.Log {
	defer trace.AddModule("_StorageAgent", "GetLogsInRange")()

	startInx, endInx := s.GetIndicesOfRange(trace, logs, tr, isSorted)

	if startInx == -1 {
		trace.DEBUG(nil, "No logs passed")
		return []*m.Log{}
	}

	trace.DEBUG(nil, endInx-startInx, "/", len(logs), " logs passed")
	return logs[startInx:endInx]
}

func (s *Selector) GetLogsOutOfRange(trace *sl.Trace, logs []*m.Log, tr m.TimeRange, isSorted bool) []*m.Log {
	defer trace.AddModule("_StorageAgent", "GetLogsOutOfRange")()

	startInx, endInx := s.GetIndicesOfRange(trace, logs, tr, isSorted)

	if startInx == -1 {
		trace.DEBUG(nil, "All logs passed")
		return logs
	}

	trace.DEBUG(nil, len(logs)-(endInx-startInx), "/", len(logs), " logs passed")

	if endInx == len(logs) {
		return logs[:startInx]
	}
	if startInx == 0 {
		return logs[endInx:]
	}
	return tools.JoinSlices(logs[:startInx], logs[endInx:])
}
