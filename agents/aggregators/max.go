package aggregators

import (
	"errors"
	"math"

	sl "github.com/j-hitgate/sherlog"

	conds "main/agents/conditions"
	aerr "main/app_errors"
	m "main/models"
)

type Max struct {
	max       int64
	column    string
	condition m.ICondition
}

func NewMax(trace *sl.Trace, args []string, values []any, lld *m.LoadLogsData) (aggr *Max, err error) {
	defer trace.AddModule("", "NewMax")()

	var condition m.ICondition

	if len(args) < 1 || len(args) > 2 {
		trace.NOTE(nil, "Number of arguments ", len(args), " must be 1-2")
		return nil, aerr.NewAppErr(aerr.BadReq, "Invalid 'max' aggregator")
	}

	column := args[0]

	if t, ok := m.GetColumnType(column); !ok || t != m.INT {
		err = aerr.NewAppErr(aerr.BadReq, "Incorrect log column in 'max' aggregator: ", column)
		trace.NOTE(nil, err.Error())
		return nil, err
	}

	if len(args) == 2 {
		condition, err = conds.ParseCondition(trace, args[1], values, nil, lld)

		if err != nil {
			return nil, err
		}
	}

	return &Max{
		max:       math.MinInt64,
		column:    column,
		condition: condition,
	}, nil
}

func (max *Max) Update(trace *sl.Trace, l *m.Log) error {
	if max.condition != nil {
		ok, err := max.condition.Check(trace, l)

		if err != nil {
			return err
		}

		if !ok {
			return nil
		}
	}

	val, ok := l.GetValue(max.column)

	if !ok {
		err := errors.New("Value for aggregator 'max' not found. Column: " + max.column)
		trace.ERROR(nil, err.Error())
		return err
	}

	if val.(int64) > max.max {
		max.max = val.(int64)
	}
	return nil
}

func (max *Max) GetResult() any {
	return max.max
}

func (max *Max) CopyDefault() m.IAggregator {
	return &Max{
		max:       math.MinInt64,
		column:    max.column,
		condition: max.condition,
	}
}

func (max *Max) Equals(other m.IAggregator) bool {
	switch other := other.(type) {
	case *Max:
		if max == nil || other == nil {
			return max == other
		}

		if max.max != other.max ||
			max.column != other.column {
			return false
		}

		if max.condition == nil || other.condition == nil {
			return max.condition == other.condition
		}
		return max.condition.Equals(other.condition)
	}
	return false
}
