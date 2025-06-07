package log_utils

import (
	sl "github.com/j-hitgate/sherlog"

	m "main/models"
	"main/tools"
)

type Groups struct {
	groupBy      string
	aggrs        map[string]m.IAggregator
	groups       map[any]map[string]m.IAggregator
	groupingVals map[any]any
	havingCond   m.ICondition
}

func NewGroups(groupBy string, aggrs map[string]m.IAggregator, havingCond m.ICondition) *Groups {
	return &Groups{
		groupBy:      groupBy,
		aggrs:        aggrs,
		groups:       map[any]map[string]m.IAggregator{},
		groupingVals: map[any]any{},
		havingCond:   havingCond,
	}
}

func (g *Groups) Update(trace *sl.Trace, l *m.Log) {
	val, _ := l.GetValue(g.groupBy)
	groupKey := tools.ToOrderedValue(val)
	aggrs, ok := g.groups[groupKey]

	if !ok {
		aggrs = map[string]m.IAggregator{}

		for key, aggr := range g.aggrs {
			aggrs[key] = aggr.CopyDefault()
		}
		g.groups[groupKey] = aggrs
		g.groupingVals[groupKey] = val
	}

	for _, aggr := range aggrs {
		aggr.Update(trace, l)
	}
}

func (g *Groups) GetAggrSources(trace *sl.Trace) ([]*m.AggrSource, error) {
	sources := []*m.AggrSource{}

	for key, aggrs := range g.groups {
		source := m.NewAggrSource(g.groupBy, g.groupingVals[key], aggrs)

		if g.havingCond != nil {
			ok, err := g.havingCond.Check(trace, source)

			if err != nil {
				return nil, err
			}

			if !ok {
				continue
			}
		}

		sources = append(sources, source)
	}

	return sources, nil
}

func (g *Groups) Equals(other *Groups) bool {
	return g.groupBy == other.groupBy &&
		g.havingCond.Equals(other.havingCond) &&
		tools.DeepEqualMaps(g.aggrs, other.aggrs) &&
		tools.EqualMapsBy(
			g.groups,
			other.groups,
			func(m1, m2 map[string]m.IAggregator) bool {
				return tools.DeepEqualMaps(m1, m2)
			},
		)
}
