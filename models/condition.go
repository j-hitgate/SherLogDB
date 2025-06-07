package models

import (
	"main/tools"
)

// Aggregators

type AggrData struct {
	GropingField  string
	SetAggregator func(string) error
}

type AggrSource struct {
	values map[string]any
}

func NewAggrSource(groupingColumn string, groupingColumnValue any, aggrs map[string]IAggregator) *AggrSource {
	values := map[string]any{groupingColumn: groupingColumnValue}

	for key, aggr := range aggrs {
		values[key] = aggr.GetResult()
	}

	return &AggrSource{values: values}
}

func (av *AggrSource) GetValue(key string) (any, bool) {
	val, ok := av.values[key]
	return val, ok
}

// Conditions

type Operant struct {
	T         ValueType
	Value     any
	SourceKey string
}

func (o *Operant) Equals(other *Operant) bool {
	if o == nil || other == nil {
		return o == other
	}

	if o.T != other.T || o.SourceKey != other.SourceKey {
		return false
	}

	if o.Value == nil || other.Value == nil {
		return o.Value == other.Value
	}

	if o.T == STR_ARRAY {
		return tools.EqualSlices(o.Value.([]string), other.Value.([]string))
	}

	if o.T == INT_ARRAY {
		return tools.EqualSlices(o.Value.([]int64), other.Value.([]int64))
	}

	return o.Value == other.Value
}
