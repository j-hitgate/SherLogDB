package conditions

import (
	sl "github.com/j-hitgate/sherlog"

	aerr "main/app_errors"
	m "main/models"
)

func ParseCondition(trace *sl.Trace, s string, values []any, ad *m.AggrData, lld *m.LoadLogsData) (m.ICondition, error) {
	defer trace.AddModule("", "ParseCondition")
	fields := sl.Fields{"condition": s}
	trace.STAGE(fields, "Condition processing...")

	i := 0
	condition, err := parseCondition(trace, s, values, &i, ad, lld)

	if err != nil {
		trace.NOTE(fields, err.Error())
		return nil, err
	}

	trace.STAGE(nil, "Condition processed")
	return condition, nil
}

func parseCondition(trace *sl.Trace, s string, values []any, i *int, ad *m.AggrData, lld *m.LoadLogsData) (condition m.ICondition, err error) {
	cp := NewConditionParser(trace)

	var comparator *Comparator

	keyChars := map[byte]bool{
		'&': true,
		'|': true,
		'!': true,
		'(': true,
		')': true,
	}
	isMainCond := *i == 0
	start := *i

	for ; *i < len(s); *i++ {
		char := s[*i]

		if !keyChars[char] {
			continue
		}

		if char == '(' || char == '!' {
			invert := char == '!'

			if invert {
				if *i+1 == len(s) {
					return nil, aerr.NewAppErr(aerr.BadReq, "'!' in end of condition")
				}
				if s[*i+1] != '(' {
					continue
				}
				*i += 2

			} else {
				*i++
			}

			condition, err = parseCondition(trace, s, values, i, ad, lld)

			if err != nil {
				return nil, err
			}

			if invert {
				condition.Invert()
			}

		} else if char == ')' {
			if isMainCond {
				return nil, aerr.NewAppErr(aerr.BadReq, "Extra bracket ')'")
			}
			break

		} else {
			if condition == nil {
				condition, err = cp.Parse(s[start:*i], values, ad, lld)

				if err != nil {
					return nil, err
				}
			}
			start = *i + 1

			if comparator != nil {
				comparator.SecondCondition = condition
				condition = comparator
			}

			operator := AND
			if char == '|' {
				operator = OR
			}

			comparator = &Comparator{
				Operator:       operator,
				FirstCondition: condition,
			}
			condition = nil
		}
	}

	if !isMainCond && *i == len(s) {
		return nil, aerr.NewAppErr(aerr.BadReq, "Extra bracket '('")
	}

	if start == len(s) {
		return nil, aerr.NewAppErr(aerr.BadReq, "Expected condition after ", s[*i])
	}

	if condition == nil {
		condition, err = cp.Parse(s[start:*i], values, ad, lld)

		if err != nil {
			return nil, err
		}
	}

	if comparator != nil {
		comparator.SecondCondition = condition
		condition = comparator
	}

	return condition, nil
}
