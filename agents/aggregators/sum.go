package aggregators

import (
	"errors"

	sl "github.com/j-hitgate/sherlog"

	conds "main/agents/conditions"
	aerr "main/app_errors"
	m "main/models"
)

type Sum struct {
	sum       int64
	column    string
	condition m.ICondition
}

func NewSum(trace *sl.Trace, args []string, values []any, lld *m.LoadLogsData) (aggr *Sum, err error) {
	defer trace.AddModule("", "NewMax")()

	var condition m.ICondition

	if len(args) < 1 || len(args) > 2 {
		trace.NOTE(nil, "Number of arguments ", len(args), " must be 1-2")
		return nil, aerr.NewAppErr(aerr.BadReq, "invalid 'sum' aggregator")
	}

	column := args[0]

	if t, ok := m.GetColumnType(column); !ok || t != m.INT {
		err = aerr.NewAppErr(aerr.BadReq, "Incorrect log column in 'sum' aggregator: ", column)
		trace.NOTE(nil, err.Error())
		return nil, err
	}

	if len(args) == 2 {
		condition, err = conds.ParseCondition(trace, args[1], values, nil, lld)

		if err != nil {
			return nil, err
		}
	}

	return &Sum{
		column:    column,
		condition: condition,
	}, nil
}

func (s *Sum) Update(trace *sl.Trace, l *m.Log) error {
	if s.condition != nil {
		ok, err := s.condition.Check(trace, l)

		if err != nil {
			return err
		}

		if !ok {
			return nil
		}
	}

	val, ok := l.GetValue(s.column)

	if !ok {
		err := errors.New("Value for aggregator 'sum' not found. Column: " + s.column)
		trace.ERROR(nil, err.Error())
		return err
	}

	s.sum += val.(int64)
	return nil
}

func (s *Sum) GetResult() any {
	return s.sum
}

func (s *Sum) CopyDefault() m.IAggregator {
	return &Sum{
		sum:       0,
		column:    s.column,
		condition: s.condition,
	}
}

func (s *Sum) Equals(other m.IAggregator) bool {
	switch other := other.(type) {
	case *Sum:
		if s == nil || other == nil {
			return s == other
		}

		if s.sum != other.sum || s.column != other.column {
			return false
		}

		if s.condition == nil || other.condition == nil {
			return s.condition == other.condition
		}
		return s.condition.Equals(other.condition)
	default:
		return false
	}
}
