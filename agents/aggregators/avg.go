package aggregators

import (
	"errors"

	sl "github.com/j-hitgate/sherlog"

	conds "main/agents/conditions"
	aerr "main/app_errors"
	m "main/models"
)

type Avg struct {
	sum       int64
	count     int64
	column    string
	condition m.ICondition
}

func NewAvg(trace *sl.Trace, args []string, values []any, lld *m.LoadLogsData) (aggr *Avg, err error) {
	defer trace.AddModule("", "NewAvg")()

	var condition m.ICondition

	if len(args) < 1 || len(args) > 2 {
		trace.NOTE(nil, "Number of arguments", len(args), "must be 1-2")
		return nil, aerr.NewAppErr(aerr.BadReq, "invalid 'avg' aggregator")
	}

	column := args[0]

	if t, ok := m.GetColumnType(column); !ok || t != m.INT {
		err = aerr.NewAppErr(aerr.BadReq, "Incorrect log column in 'avg' aggregator: ", column)
		trace.NOTE(nil, err.Error())
		return nil, err
	}

	if len(args) == 2 {
		condition, err = conds.ParseCondition(trace, args[1], values, nil, lld)

		if err != nil {
			return nil, err
		}
	}

	return &Avg{
		column:    column,
		condition: condition,
	}, nil
}

func (avg *Avg) Update(trace *sl.Trace, l *m.Log) error {
	if avg.condition != nil {
		ok, err := avg.condition.Check(trace, l)

		if err != nil {
			return err
		}

		if !ok {
			return nil
		}
	}

	val, ok := l.GetValue(avg.column)

	if !ok {
		err := errors.New("Value for aggregator 'avg' not found. Column: " + avg.column)
		trace.ERROR(nil, err.Error())
		return err
	}

	avg.sum += val.(int64)
	avg.count++
	return nil
}

func (avg *Avg) GetResult() any {
	return avg.sum / avg.count
}

func (avg *Avg) CopyDefault() m.IAggregator {
	return &Avg{
		sum:       0,
		count:     0,
		column:    avg.column,
		condition: avg.condition,
	}
}

func (avg *Avg) Equals(other m.IAggregator) bool {
	switch other := other.(type) {
	case *Avg:
		if avg == nil || other == nil {
			return avg == other
		}

		if avg.sum != other.sum ||
			avg.count != other.count ||
			avg.column != other.column {
			return false
		}

		if avg.condition == nil || other.condition == nil {
			return avg.condition == other.condition
		}
		return avg.condition.Equals(other.condition)
	}
	return false
}
