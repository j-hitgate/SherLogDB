package conditions

import (
	sl "github.com/j-hitgate/sherlog"

	m "main/models"
)

type CmprOperator byte

const (
	AND CmprOperator = iota
	OR
)

type Comparator struct {
	Operator        CmprOperator
	FirstCondition  m.ICondition
	SecondCondition m.ICondition
	invert          bool
}

func (c *Comparator) Check(trace *sl.Trace, source m.IConditionSource) (bool, error) {
	result, err := c.FirstCondition.Check(trace, source)

	if err != nil {
		return false, err
	}

	expected := c.Operator == OR

	if result != expected {
		result, err = c.SecondCondition.Check(trace, source)

		if err != nil {
			return false, err
		}
	}

	if c.invert {
		result = !result
	}
	return result, nil
}

func (c *Comparator) Invert() {
	c.invert = !c.invert
}

func (c *Comparator) Equals(other m.ICondition) bool {
	switch other := other.(type) {
	case *Comparator:
		if c == nil || other == nil {
			return c == other
		}

		return c.Operator == other.Operator &&
			c.FirstCondition.Equals(other.FirstCondition) &&
			c.SecondCondition.Equals(other.SecondCondition)
	}
	return false
}
