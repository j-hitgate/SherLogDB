package conditions

import (
	"errors"
	"strconv"
	"strings"

	sl "github.com/j-hitgate/sherlog"

	aerr "main/app_errors"
	m "main/models"
	"main/tools"
)

// Types and data

type Operator byte

const (
	EQUAL Operator = iota
	NOT_EQUAL
	GEATER_THAN
	GEATER_EQUAL
	LESS_THAN
	LESS_EQUAL
	IN
)

// ConditionParser

type ConditionParser struct {
	trace *sl.Trace
}

func NewConditionParser(trace *sl.Trace) *ConditionParser {
	return &ConditionParser{trace}
}

func (cp *ConditionParser) splitConditionStr(s string) (operant1, operant2 string, operator Operator, err error) {
	popModule := cp.trace.AddModule("_ConditionParser", "splitConditionStr")
	defer func() {
		if err != nil {
			cp.trace.NOTE(nil, err.Error())
		}
		popModule()
	}()

	operators := []struct {
		val  string
		oper Operator
	}{
		{"==", EQUAL},
		{">=", GEATER_EQUAL},
		{">", GEATER_THAN},
		{"!=", NOT_EQUAL},
		{"<=", LESS_EQUAL},
		{"<", LESS_THAN},
		{"=>", IN},
	}
	longerOper := 2

	operFirstChars := map[byte]bool{
		'>': true,
		'<': true,
		'=': true,
		'!': true,
	}

	i := 0

	for ; i < len(s); i++ {
		if operFirstChars[s[i]] {
			break
		}
	}

	if i+longerOper > len(s) {
		err = aerr.NewAppErr(aerr.BadReq, "Incorrect condition: ", s)
		return "", "", 0, err
	}

	subCond := s[i : i+longerOper]
	end := -1

	for _, op := range operators {
		if strings.HasPrefix(subCond, op.val) {
			operator = op.oper
			end = i + len(op.val)
			break
		}
	}

	if end == -1 {
		err = aerr.NewAppErr(aerr.BadReq, "Expected operator: ", s)
		return "", "", 0, err
	}

	operant1 = strings.TrimSpace(s[:i])
	operant2 = strings.TrimSpace(s[end:])

	if operant1 == "" || operant2 == "" {
		err = aerr.NewAppErr(aerr.BadReq, "Expected operant: ", s)
		return "", "", 0, err
	}

	cp.trace.DEBUG(sl.Fields{
		"operand 1": operant1,
		"operand 2": operant2,
		"operator":  s[i:end],
	}, "Operator and operators found")

	return operant1, operant2, operator, nil
}

func (*ConditionParser) floatToIntArray(arr []any) []int64 {
	newArr := make([]int64, len(arr))

	for i, val := range arr {
		newArr[i] = int64(val.(float64))
	}

	return newArr
}

func (cp *ConditionParser) arrayToOperant(arr []any, s string) (*m.Operant, error) {
	if len(arr) == 0 {
		return nil, aerr.NewAppErr(aerr.BadReq, "Array is empty: ", s)
	}

	switch arr[0].(type) {
	case string:
		if !tools.CheckSliceItemsType[string](arr) {
			return nil, aerr.NewAppErr(aerr.BadReq, "Incorrect item type in array: ", s)
		}
		return &m.Operant{Value: tools.CastSlice[string](arr), T: m.STR_ARRAY}, nil
	case float64:
		if !tools.CheckSliceItemsType[float64](arr) {
			return nil, aerr.NewAppErr(aerr.BadReq, "Incorrect item type in array: ", s)
		}
		return &m.Operant{Value: cp.floatToIntArray(arr), T: m.INT_ARRAY}, nil
	default:
		return nil, aerr.NewAppErr(aerr.BadReq, "Invalid array item type: ", s)
	}
}

func (cp *ConditionParser) getOperant(s string, values []any, ad *m.AggrData, lld *m.LoadLogsData) (oper *m.Operant, err error) {
	popModule := cp.trace.AddModule("_ConditionParser", "getOperant")
	defer func() {
		cp.trace.DEBUG_if_not_err(err, nil, "Operand getted: ", s)
		popModule()
	}()

	if len(s) < 2 {
		err = aerr.NewAppErr(aerr.BadReq, "Incorrect operant: ", s)
		return nil, err
	}

	if s[0] == '?' {
		// Find value

		num, err := strconv.ParseInt(s[1:], 10, 32)
		index := int(num)

		if err != nil || index < 0 || index >= len(values) {
			err = aerr.NewAppErr(aerr.BadReq, "Incorrect index: ", s)
			return nil, err
		}
		val := values[index]

		// Cast value

		switch val := val.(type) {
		case string:
			return &m.Operant{Value: val, T: m.STR}, nil
		case float64:
			return &m.Operant{Value: int64(val), T: m.INT}, nil
		case []any:
			oper, err = cp.arrayToOperant(val, s)
			return oper, err
		default:
			err = aerr.NewAppErr(aerr.BadReq, "Invalid operant type: ", s)
			return nil, err
		}
	}

	if ad == nil {
		// Field key
		if t, ok := m.GetColumnType(s); ok {
			if lld != nil {
				lld.Columns[s] = true
			}
			return &m.Operant{SourceKey: s, T: t}, nil
		}

	} else {
		// aggregator key
		if s == ad.GropingField {
			t, ok := m.GetColumnType(s)

			if !ok {
				err = aerr.NewAppErr(aerr.BadReq, "Invalid grouping field: ", ad.GropingField)
				return nil, err
			}

			if lld != nil {
				lld.Columns[s] = true
			}
			return &m.Operant{SourceKey: s, T: t}, nil
		}

		i := strings.IndexByte(s, '[')

		if i > -1 && s[len(s)-1] == ']' {
			aggrName := s[:i]

			if t, ok := m.GetAggrType(aggrName); ok {
				if err = ad.SetAggregator(s); err != nil {
					return nil, err
				}
				return &m.Operant{SourceKey: s, T: t}, nil
			}
			err = aerr.NewAppErr(aerr.BadReq, "Incorrect aggregator: ", s)
			return nil, err
		}
	}

	err = aerr.NewAppErr(aerr.BadReq, "Incorrect operant: ", s)
	return nil, err
}

func (cp *ConditionParser) Parse(s string, values []any, ad *m.AggrData, lld *m.LoadLogsData) (cond *Condition, err error) {
	popModule := cp.trace.AddModule("_ConditionParser", "Parse")
	defer func() {
		if err != nil {
			cp.trace.NOTE(nil, err.Error())
		} else {
			cp.trace.DEBUG(nil, "Condition parsed: ", s)
		}
		popModule()
	}()

	strOper1, strOper2, operator, err := cp.splitConditionStr(s)

	if err != nil {
		return nil, err
	}

	oper1, err := cp.getOperant(strOper1, values, ad, lld)

	if err != nil {
		return nil, err
	}

	oper2, err := cp.getOperant(strOper2, values, ad, lld)

	if err != nil {
		return nil, err
	}

	if operator == IN {
		if !((oper1.T == m.STR && oper2.T == m.STR_ARRAY) ||
			(oper1.T == m.INT && oper2.T == m.INT_ARRAY)) {
			err = aerr.NewAppErr(aerr.BadReq, "Incompatible types of operators: ", s)
			return nil, err
		}
	} else {
		if oper1.T != oper2.T {
			err = aerr.NewAppErr(aerr.BadReq, "Different types of operators: ", s)
			return nil, err
		}

		if oper1.T == m.STR_MAP {
			err = aerr.NewAppErr(aerr.BadReq, "Invalid type of operators: ", s)
			return nil, err
		}

		if (operator == GEATER_THAN ||
			operator == GEATER_EQUAL ||
			operator == LESS_THAN ||
			operator == LESS_EQUAL) &&
			oper1.T != m.INT {
			err = aerr.NewAppErr(aerr.BadReq, "Incorrect operator type: ", s)
			return nil, err
		}
	}

	return &Condition{
		oper1:    oper1,
		oper2:    oper2,
		operator: operator,
	}, nil
}

// Condition

type Condition struct {
	oper1, oper2 *m.Operant
	operator     Operator
	invert       bool
}

func (c *Condition) condition(val1, val2 any) (bool, error) {
	switch c.operator {
	case EQUAL, NOT_EQUAL:
		expected := c.operator == EQUAL

		if c.oper1.T == m.STR_ARRAY {
			return tools.EqualSlices(val1.([]string), val2.([]string)) == expected, nil
		}
		if c.oper1.T == m.INT_ARRAY {
			return tools.EqualSlices(val1.([]int64), val2.([]int64)) == expected, nil
		}
		return (val1 == val2) == expected, nil
	case GEATER_THAN:
		return val1.(int64) > val2.(int64), nil
	case GEATER_EQUAL:
		return val1.(int64) >= val2.(int64), nil
	case LESS_THAN:
		return val1.(int64) < val2.(int64), nil
	case LESS_EQUAL:
		return val1.(int64) <= val2.(int64), nil
	case IN:
		if c.oper1.T == m.STR {
			return tools.Contains(val1.(string), val2.([]string)), nil
		}
		if c.oper1.T == m.INT {
			return tools.Contains(val1.(int64), val2.([]int64)), nil
		}
	}
	return false, errors.New("incorrect operator")
}

func (c *Condition) Check(trace *sl.Trace, source m.IConditionSource) (bool, error) {
	opers := []*m.Operant{c.oper1, c.oper2}
	values := make([]any, 2)

	for i := range opers {
		if opers[i].SourceKey != "" {
			val, ok := source.GetValue(opers[i].SourceKey)

			if !ok {
				errMsg := "Condition source not found: " + opers[i].SourceKey
				trace.ERROR(sl.WithFields(
					"operant 1", c.oper1,
					"operant 2", c.oper2,
					"operator", c.operator,
				), errMsg)
				return false, errors.New(errMsg)
			}

			values[i] = val
		} else {
			values[i] = opers[i].Value
		}
	}

	result, err := c.condition(values[0], values[1])

	if err != nil {
		trace.ERROR(sl.WithFields(
			"operant 1", c.oper1,
			"operant 2", c.oper2,
			"operator", c.operator,
		), err.Error())
	}

	if c.invert {
		result = !result
	}
	return result, err
}

func (c *Condition) Invert() {
	c.invert = !c.invert
}

func (c *Condition) Equals(other m.ICondition) bool {
	switch other := other.(type) {
	case *Condition:
		if c == nil || other == nil {
			return c == other
		}

		return c.operator == other.operator &&
			c.oper1.Equals(other.oper1) &&
			c.oper2.Equals(other.oper2)
	}
	return false
}
