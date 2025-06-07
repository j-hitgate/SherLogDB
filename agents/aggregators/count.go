package aggregators

import (
	sl "github.com/j-hitgate/sherlog"

	conds "main/agents/conditions"
	aerr "main/app_errors"
	m "main/models"
)

type Count struct {
	count     int64
	condition m.ICondition
}

func NewCount(trace *sl.Trace, args []string, values []any, lld *m.LoadLogsData) (aggr *Count, err error) {
	defer trace.AddModule("", "NewCount")()

	var condition m.ICondition

	if len(args) > 1 {
		trace.NOTE(nil, "Number of arguments ", len(args), " must be 0-1")
		return nil, aerr.NewAppErr(aerr.BadReq, "Extra arguments in 'count' aggregator")
	}

	if len(args) == 1 {
		condition, err = conds.ParseCondition(trace, args[0], values, nil, lld)

		if err != nil {
			return nil, err
		}
	}

	return &Count{condition: condition}, nil
}

func (c *Count) Update(trace *sl.Trace, l *m.Log) error {
	if c.condition != nil {
		ok, err := c.condition.Check(trace, l)

		if err != nil {
			return err
		}

		if !ok {
			return nil
		}
	}

	c.count++
	return nil
}

func (c *Count) GetResult() any {
	return c.count
}

func (c *Count) CopyDefault() m.IAggregator {
	return &Count{
		count:     0,
		condition: c.condition,
	}
}

func (c *Count) Equals(other m.IAggregator) bool {
	switch other := other.(type) {
	case *Count:
		if c == nil || other == nil {
			return c == other
		}

		if c.count != other.count {
			return false
		}

		if c.condition == nil || other.condition == nil {
			return c.condition == other.condition
		}
		return c.condition.Equals(other.condition)
	}
	return false
}
