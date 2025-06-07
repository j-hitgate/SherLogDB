package models

import (
	sl "github.com/j-hitgate/sherlog"
)

// Condition

type IConditionSource interface {
	GetValue(string) (any, bool)
}

type ICondition interface {
	Check(*sl.Trace, IConditionSource) (bool, error)
	Invert()
	Equals(ICondition) bool
}

// Aggregator

type IAggregator interface {
	Update(*sl.Trace, *Log) error
	GetResult() any
	CopyDefault() IAggregator
	Equals(IAggregator) bool
}
