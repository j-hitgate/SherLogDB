package time_range

import (
	"strconv"
	"strings"
	"time"

	sl "github.com/j-hitgate/sherlog"

	aerr "main/app_errors"
	m "main/models"
)

type Parser struct {
	trace *sl.Trace
}

func NewParser(trace *sl.Trace) *Parser {
	return &Parser{trace}
}

var timeSuff = map[byte]time.Duration{
	'w': time.Hour * 24 * 7,
	'd': time.Hour * 24,
	'h': time.Hour,
	'm': time.Minute,
	's': time.Second,
}

func (*Parser) ParseDuration(source string) (time.Duration, error) {
	var duration time.Duration = 0

	parts := strings.Split(source, " ")

	for _, part := range parts {
		last := len(part) - 1
		dur, ok := timeSuff[part[last]]

		if !ok {
			return 0, aerr.NewAppErr(aerr.BadReq, "Not time suffix: ", part)
		}

		num, err := strconv.ParseInt(part[:last], 10, 32)

		if err != nil || num < 1 {
			return 0, aerr.NewAppErr(aerr.BadReq, "Not number: ", part)
		}

		duration += time.Duration(num) * dur
	}

	return duration, nil
}

func (p *Parser) parseLastRange(value string) (tr m.TimeRange, err error) {
	delta, err := p.ParseDuration(value)

	if err != nil {
		return tr, aerr.NewAppErr(aerr.BadReq, "Invalid last timestamp: ", value)
	}

	tr.Start = time.Now().Add(-delta).UnixMilli()
	return tr, nil
}

func (*Parser) parseRelativeRange(option, value string) (tr m.TimeRange, err error) {
	ts, err := strconv.ParseInt(value, 10, 64)

	if err != nil {
		return tr, aerr.NewAppErr(aerr.BadReq, "Not timestamp: ", option, " ", value)
	}

	if option == "before" {
		tr.End = ts
	} else {
		tr.Start = ts
	}

	return tr, nil
}

func (*Parser) parseSimpleRange(timeRange string) (tr m.TimeRange, err error) {
	parts := strings.Split(timeRange, " - ")

	if len(parts) != 2 {
		return tr, aerr.NewAppErr(aerr.BadReq, "Incorrect 'time_range': ", timeRange)
	}

	parts[0] = strings.TrimSpace(parts[0])
	tr.Start, err = strconv.ParseInt(parts[0], 10, 64)

	if err != nil {
		return tr, aerr.NewAppErr(aerr.BadReq, "Invalid start timestamp: ", parts[0])
	}

	parts[1] = strings.TrimSpace(parts[1])
	tr.End, err = strconv.ParseInt(parts[1], 10, 64)

	if err != nil {
		return tr, aerr.NewAppErr(aerr.BadReq, "Invalid end timestamp: ", parts[1])
	}

	if tr.End < tr.Start {
		return tr, aerr.NewAppErr(aerr.BadReq, "Start after end timestamp: ", timeRange)
	}

	return tr, nil
}

func (p *Parser) Parse(timeRange string) (tr m.TimeRange, err error) {
	popModule := p.trace.AddModule("_Parser", "Parse")
	defer func() {
		if err != nil {
			p.trace.NOTE(nil, err.Error())
		} else {
			p.trace.DEBUG(nil, "Time range parsed: ", timeRange)
		}
		popModule()
	}()

	if timeRange == "" {
		return tr, nil
	}

	i := strings.IndexByte(timeRange, ' ')

	if i == -1 {
		err = aerr.NewAppErr(aerr.BadReq, "Incorrect 'time_range': ", timeRange)
		return tr, err
	}

	option := timeRange[:i]

	switch option {
	case "last":
		tr, err = p.parseLastRange(timeRange[i+1:])
	case "before", "after":
		tr, err = p.parseRelativeRange(option, timeRange[i+1:])
	default:
		tr, err = p.parseSimpleRange(timeRange)
	}
	return tr, err
}
