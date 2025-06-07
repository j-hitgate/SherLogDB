package aggregators

import (
	"strings"

	sl "github.com/j-hitgate/sherlog"

	aerr "main/app_errors"
	m "main/models"
)

func ParseAggregator(trace *sl.Trace, s string, values []any, lld *m.LoadLogsData) (aggr m.IAggregator, err error) {
	defer trace.AddModule("", "ParseAggregator")()

	i := strings.IndexRune(s, '[')

	if i == -1 || s[len(s)-1] != ']' {
		err := aerr.NewAppErr(aerr.BadReq, "Invalid aggregator: ", s)
		trace.NOTE(nil, err.Error())
		return nil, err
	}
	aggrName := s[:i]
	i++

	args := strings.Split(s[i:len(s)-1], ",")

	if len(args) == 1 && args[0] == "" {
		args = []string{}
	}

	switch aggrName {
	case m.AG_COUNT:
		aggr, err = NewCount(trace, args, values, lld)
	case m.AG_AVG:
		aggr, err = NewAvg(trace, args, values, lld)
	case m.AG_MAX:
		aggr, err = NewMax(trace, args, values, lld)
	case m.AG_MIN:
		aggr, err = NewMin(trace, args, values, lld)
	case m.AG_SUM:
		aggr, err = NewSum(trace, args, values, lld)
	default:
		err = aerr.NewAppErr(aerr.BadReq, "Incorrect aggregator: ", s)
	}
	fields := sl.Fields{"aggr": s}

	if err != nil {
		trace.NOTE(fields, err.Error())
		return nil, err
	}

	trace.DEBUG(fields, "Aggregator parsed")
	return aggr, nil
}
