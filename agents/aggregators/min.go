package aggregators

import (
	"errors"
	"math"

	sl "github.com/j-hitgate/sherlog"

	conds "main/agents/conditions"
	aerr "main/app_errors"
	m "main/models"
)

type Min struct {
	min       int64
	column    string
	condition m.ICondition
}

func NewMin(trace *sl.Trace, args []string, values []any, lld *m.LoadLogsData) (aggr *Min, err error) {
	defer trace.AddModule("", "NewMin")()

	var condition m.ICondition

	if len(args) < 1 || len(args) > 2 {
		trace.NOTE(nil, "Number of arguments ", len(args), " must be 1-2")
		return nil, aerr.NewAppErr(aerr.BadReq, "Invalid 'min' aggregator")
	}

	column := args[0]

	if t, ok := m.GetColumnType(column); !ok || t != m.INT {
		err = aerr.NewAppErr(aerr.BadReq, "Incorrect log column in 'min' aggregator: ", column)
		trace.NOTE(nil, err.Error())
		return nil, err
	}

	if len(args) == 2 {
		condition, err = conds.ParseCondition(trace, args[1], values, nil, lld)

		if err != nil {
			return nil, err
		}
	}

	return &Min{
		min:       math.MaxInt64,
		column:    column,
		condition: condition,
	}, nil
}

func (min *Min) Update(trace *sl.Trace, l *m.Log) error {
	if min.condition != nil {
		ok, err := min.condition.Check(trace, l)

		if err != nil {
			return err
		}

		if !ok {
			return nil
		}
	}

	val, ok := l.GetValue(min.column)

	if !ok {
		err := errors.New("Value for aggregator 'min' not found. Column: " + min.column)
		trace.ERROR(nil, err.Error())
		return err
	}

	if val.(int64) < min.min {
		min.min = val.(int64)
	}
	return nil
}

func (min *Min) GetResult() any {
	return min.min
}

func (min *Min) CopyDefault() m.IAggregator {
	return &Min{
		min:       math.MaxInt64,
		column:    min.column,
		condition: min.condition,
	}
}

func (min *Min) Equals(other m.IAggregator) bool {
	switch other := other.(type) {
	case *Min:
		if min == nil || other == nil {
			return min == other
		}

		if min.min != other.min ||
			min.column != other.column {
			return false
		}

		if min.condition == nil || other.condition == nil {
			return min.condition == other.condition
		}
		return min.condition.Equals(other.condition)
	}
	return false
}
