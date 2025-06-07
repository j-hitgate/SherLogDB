package conditions

import (
	"errors"
	"testing"

	sl "github.com/j-hitgate/sherlog"
	"github.com/stretchr/testify/assert"

	m "main/models"
	tt "main/test_tools"
)

func TestSplitConditionStr(t *testing.T) {
	testCases := []struct {
		name      string
		condition string
		operant1  string
		operant2  string
		operator  Operator
		hasErr    bool
	}{
		{
			name:      "el",
			condition: " A ==  B ",
			operant1:  "A",
			operant2:  "B",
			operator:  EQUAL,
		},
		{
			name:      "ne",
			condition: " A!=  B",
			operant1:  "A",
			operant2:  "B",
			operator:  NOT_EQUAL,
		},
		{
			name:      "le",
			condition: "A <= B",
			operant1:  "A",
			operant2:  "B",
			operator:  LESS_EQUAL,
		},
		{
			name:      "lt",
			condition: "A < B ",
			operant1:  "A",
			operant2:  "B",
			operator:  LESS_THAN,
		},
		{
			name:      "ge",
			condition: " A>=B  ",
			operant1:  "A",
			operant2:  "B",
			operator:  GEATER_EQUAL,
		},
		{
			name:      "gt",
			condition: "A  >B  ",
			operant1:  "A",
			operant2:  "B",
			operator:  GEATER_THAN,
		},
		{
			name:      "in",
			condition: "A=>B",
			operant1:  "A",
			operant2:  "B",
			operator:  IN,
		},
		{
			name:      "error1",
			condition: " A  =    B  ",
			hasErr:    true,
		},
		{
			name:      "error2",
			condition: "  ==    B  ",
			hasErr:    true,
		},
		{
			name:      "error3",
			condition: ">",
			hasErr:    true,
		},
		{
			name:      "error4",
			condition: "",
			hasErr:    true,
		},
	}

	tt.SherlogInit()
	cp := NewConditionParser(sl.NewTrace("Main"))

	for _, tc := range testCases {
		oper1, oper2, operator, err := cp.splitConditionStr(tc.condition)

		if err != nil {
			assert.True(t, tc.hasErr, tc.name)
			continue
		}
		assert.False(t, tc.hasErr, tc.name)
		assert.Equal(t, tc.operant1, oper1, tc.name)
		assert.Equal(t, tc.operant2, oper2, tc.name)
		assert.Equal(t, tc.operator, operator, tc.name)
	}
}

func TestGetOperant(t *testing.T) {
	testCases := []struct {
		name    string
		s       string
		values  []any
		ad      *m.AggrData
		operant *m.Operant
		hasErr  bool
	}{
		{
			name:    "value string",
			s:       "?0",
			values:  []any{"abc"},
			operant: &m.Operant{Value: "abc", T: m.STR},
		},
		{
			name:    "empty value string",
			s:       "?0",
			values:  []any{""},
			operant: &m.Operant{Value: "", T: m.STR},
		},
		{
			name:    "value integer",
			s:       "?1",
			values:  []any{2.0, 5.0},
			operant: &m.Operant{Value: int64(5), T: m.INT},
		},
		{
			name:    "string array",
			s:       "?1",
			values:  []any{"", []any{"ab", "cd"}},
			operant: &m.Operant{Value: []string{"ab", "cd"}, T: m.STR_ARRAY},
		},
		{
			name:    "integer array",
			s:       "?0",
			values:  []any{[]any{2.0, 5.0}, 2.0},
			operant: &m.Operant{Value: []int64{2, 5}, T: m.INT_ARRAY},
		},
		{
			name:    "field",
			s:       "level",
			values:  []any{},
			operant: &m.Operant{SourceKey: "level", T: m.INT},
		},
		{
			name:   "aggregator",
			s:      "count[5==5]",
			values: []any{},
			ad: &m.AggrData{SetAggregator: func(s string) error {
				if s != "count[5==5]" {
					return errors.New("incorrect value to set")
				}
				return nil
			}},
			operant: &m.Operant{SourceKey: "count[5==5]", T: m.INT},
		},
		{
			name:   "error: out of range",
			s:      "?2",
			values: []any{"a"},
			hasErr: true,
		},
		{
			name:   "error: this is not integer array",
			s:      "?0",
			values: []any{[]any{1.0, 2.0, "a"}},
			hasErr: true,
		},
		{
			name:   "error: incorrect log field",
			s:      "abc",
			values: []any{},
			hasErr: true,
		},
		{
			name:   "error: expected parameter index",
			s:      "?",
			values: []any{"a"},
			hasErr: true,
		},
		{
			name:   "error: incorrect parameter index",
			s:      "?abc",
			values: []any{"a"},
			hasErr: true,
		},
		{
			name:   "error: incorect aggregator",
			s:      "abc(field)",
			values: []any{},
			hasErr: true,
		},
	}

	tt.SherlogInit()
	cp := NewConditionParser(sl.NewTrace("Main"))
	lld := m.NewLoadLogsData()

	for _, tc := range testCases {
		operant, err := cp.getOperant(tc.s, tc.values, tc.ad, lld)

		if err != nil {
			assert.True(t, tc.hasErr, tc.name, err.Error())
			continue
		}
		assert.False(t, tc.hasErr, tc.name)

		assert.True(t, tc.operant.Equals(operant), tc.name)
	}
}

func TestNewCondition(t *testing.T) {
	testCases := []struct {
		name      string
		s         string
		values    []any
		ad        *m.AggrData
		condition *Condition
		hasErr    bool
	}{
		{
			name:   "equals operator and operants",
			s:      "level > ?0",
			values: []any{2.0},
			condition: &Condition{
				oper1:    &m.Operant{SourceKey: "level", T: m.INT},
				oper2:    &m.Operant{Value: int64(2), T: m.INT},
				operator: GEATER_THAN,
			},
		},
		{
			name:   "IN operator",
			s:      "?1 => modules",
			values: []any{2.0, "abc"},
			condition: &Condition{
				oper1:    &m.Operant{Value: "abc", T: m.STR},
				oper2:    &m.Operant{SourceKey: "modules", T: m.STR_ARRAY},
				operator: IN,
			},
		},
		{
			name:   "error: incorrect type for IN operator",
			s:      "?1 => level",
			values: []any{2.0, "abc"},
			hasErr: true,
		},
		{
			name:   "error: operants type not same",
			s:      "?0 >= ?1",
			values: []any{5.0, "ab"},
			hasErr: true,
		},
		{
			name:   "error: incorrect type for operator for integers",
			s:      "?0 >= ?1",
			values: []any{"ab", "cd"},
			hasErr: true,
		},
	}

	tt.SherlogInit()
	cp := NewConditionParser(sl.NewTrace("Main"))
	lld := m.NewLoadLogsData()

	for _, tc := range testCases {
		condition, err := cp.Parse(tc.s, tc.values, tc.ad, lld)

		if err != nil {
			assert.True(t, tc.hasErr, tc.name)
			continue
		}
		assert.False(t, tc.hasErr, tc.name)
		assert.True(t, tc.condition.Equals(condition), tc.name)
	}
}
