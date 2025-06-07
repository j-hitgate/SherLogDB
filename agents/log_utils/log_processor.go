package log_utils

import (
	"errors"
	"sort"
	"strings"

	sl "github.com/j-hitgate/sherlog"

	aggregs "main/agents/aggregators"
	conds "main/agents/conditions"
	"main/agents/time_range"
	aerr "main/app_errors"
	m "main/models"
	"main/tools"
)

type Processor struct {
	logPacks  [][]*m.Log
	query     *m.SearchQuery
	whereCond m.ICondition
	aggrs     map[string]m.IAggregator
	groups    *Groups
	offset    uint
	limit     uint
	trace     *sl.Trace
}

func NewProcessor(trace *sl.Trace, query *m.SearchQuery) (proc *Processor, lld *m.LoadLogsData, err error) {
	defer trace.AddModule("", "NewSearcher")()
	trace.STAGE(nil, "Query processing...")

	lld = m.NewLoadLogsData()

	// Storage
	if query.Storage == "" {
		err = aerr.NewAppErr(aerr.BadReq, "'storage' not specified")
		trace.NOTE(nil, err.Error())
		return nil, nil, err
	}
	lld.Storage = query.Storage

	// Time range
	trp := time_range.NewParser(trace)
	lld.TimeRange, err = trp.Parse(query.TimeRange)

	if err != nil {
		return nil, nil, err
	}

	// Where condition
	var whereCond m.ICondition

	if query.Where != "" {
		whereCond, err = conds.ParseCondition(trace, query.Where, query.WhereValues, nil, lld)

		if err != nil {
			return nil, nil, err
		}
	}

	// Select entries
	aggrs := map[string]m.IAggregator{}

	for _, entry := range query.Select {
		entry = strings.TrimSpace(entry)

		if entry[len(entry)-1] == ']' {
			aggrs[entry], err = aggregs.ParseAggregator(trace, entry, query.AggregValues, lld)

			if err != nil {
				return nil, nil, err
			}

		} else if _, ok := m.GetColumnType(entry); ok {
			if query.GroupBy != "" && query.GroupBy != entry {
				err = aerr.NewAppErr(aerr.BadReq, "Incorrect column in 'select': ", entry)
				trace.NOTE(nil, err.Error())
				return nil, nil, err
			}
			lld.Columns[entry] = true
		}
	}

	// GroupBy and Having condition
	var groups *Groups

	if query.GroupBy != "" {
		if _, ok := m.GetColumnType(query.GroupBy); !ok || query.GroupBy == m.C_FIELDS {
			err = aerr.NewAppErr(aerr.BadReq, "Incorrect column 'group_by': ", query.GroupBy)
			trace.NOTE(nil, err.Error())
			return nil, nil, err
		}
		lld.Columns[query.GroupBy] = true
		var havingCond m.ICondition

		if query.Having != "" {
			ad := &m.AggrData{
				GropingField: query.GroupBy,
				SetAggregator: func(s string) error {
					if _, ok := aggrs[s]; ok {
						return nil
					}
					aggrs[s], err = aggregs.ParseAggregator(trace, s, query.AggregValues, lld)
					return err
				},
			}
			havingCond, err = conds.ParseCondition(trace, query.Having, query.HavingValues, ad, lld)

			if err != nil {
				return nil, nil, err
			}
		}

		groups = NewGroups(query.GroupBy, aggrs, havingCond)
	}

	// Order
	if query.OrderBy != "" {
		key := query.OrderBy

		if key[0] == '-' {
			key = key[1:]
		}
		isPassed := true

		if key == m.C_FIELDS {
			isPassed = false
		} else if query.GroupBy == "" {
			_, isPassed = m.GetColumnType(key)
		} else {
			_, ok := aggrs[key]
			isPassed = ok || query.GroupBy == key
		}

		if !isPassed {
			err = aerr.NewAppErr(aerr.BadReq, "Incorrect column 'order_by': ", query.OrderBy)
			trace.NOTE(nil, err.Error())
			return nil, nil, err
		}
	}

	trace.STAGE(nil, "Query processed")

	return &Processor{
		logPacks:  [][]*m.Log{},
		query:     query,
		whereCond: whereCond,
		aggrs:     aggrs,
		groups:    groups,
		offset:    query.Offset,
		limit:     query.Limit,
		trace:     trace,
	}, lld, nil
}

// Logs

func (*Processor) mergeLogPacks(logPacks [][]*m.Log) []*m.Log {
	logs := tools.JoinSlices(logPacks...)

	sort.SliceStable(logs, func(i, j int) bool {
		return logs[i].Timestamp < logs[j].Timestamp
	})
	return logs
}

func (p *Processor) sortAndJoinLogPacks(desc bool) []*m.Log {
	if len(p.logPacks) == 0 {
		return []*m.Log{}
	}

	sort.Slice(p.logPacks, func(i, j int) bool {
		return p.logPacks[i][0].Timestamp < p.logPacks[j][0].Timestamp
	})

	logPacks := make([][]*m.Log, 0, len(p.logPacks))
	crossed := [][]*m.Log{}

	isNotCrossed := func(pack1, pack2 []*m.Log) bool {
		return pack1[len(pack1)-1].Timestamp < pack2[0].Timestamp
	}

	for i := 1; i < len(p.logPacks); i++ {
		pack1, pack2 := p.logPacks[i-1], p.logPacks[i]

		if isNotCrossed(pack1, pack2) {
			if len(crossed) == 0 {
				logPacks = append(logPacks, pack1)
			} else {
				logPacks = append(logPacks, p.mergeLogPacks(crossed))
				crossed = [][]*m.Log{}
			}

		} else {
			if len(crossed) == 0 {
				crossed = append(crossed, pack1)
			}
			crossed = append(crossed, pack2)
		}
	}

	if len(crossed) > 0 {
		logPacks = append(logPacks, p.mergeLogPacks(crossed))
	} else {
		logPacks = append(logPacks, p.logPacks[len(p.logPacks)-1])
	}

	if desc {
		return tools.JoinSlices(logPacks...)
	}
	return tools.JoinSlicesRevers(logPacks...)
}

func (p *Processor) sortAndGetLogs() ([]*m.Log, error) {
	defer p.trace.AddModule("_Searcher", "sortAndGetLogs")()

	if len(p.logPacks) == 0 {
		p.trace.DEBUG(nil, "Return no logs")
		return []*m.Log{}, nil
	}

	key := p.query.OrderBy
	desk := true

	if len(key) > 0 && key[0] == '-' {
		desk = false
		key = key[1:]
	}

	if key == "" || key == m.C_TIMESTAMP {
		return p.sortAndJoinLogPacks(desk), nil
	}

	logs := tools.JoinSlices(p.logPacks...)
	val, _ := logs[0].GetValue(key)

	switch val.(type) {

	case string:
		tools.SortWithKey(logs, func(l *m.Log) string {
			val, _ := l.GetValue(key)
			return val.(string)
		}, desk)

	case int64:
		tools.SortWithKey(logs, func(l *m.Log) int64 {
			val, _ := l.GetValue(key)
			return val.(int64)
		}, desk)

	case []string:
		tools.SortWithKey(logs, func(l *m.Log) string {
			val, _ := l.GetValue(key)
			return tools.ToOrderedValue(val).(string)
		}, desk)

	default:
		err := aerr.NewAppErr(aerr.BadReq, "Cannot order by column: ", key)
		p.trace.NOTE(nil, err.Error())
		return nil, err
	}

	p.trace.DEBUG(nil, "Logs ordered by column: ", p.query.OrderBy)
	return logs, nil
}

func (p *Processor) getResultFromLogs() (rows [][]any, err error) {
	defer p.trace.AddModule("_Searcher", "getResultFromLogs")()
	p.trace.STAGE(nil, "Getting results...")

	logs, err := p.sortAndGetLogs()

	if err != nil {
		return nil, err
	}

	// Get log slice

	if p.offset >= uint(len(logs)) {
		return [][]any{}, nil
	}
	end := int(p.offset + p.limit)

	if p.limit == 0 || end > len(logs) {
		end = len(logs)
	}
	logs = logs[p.offset:end]

	// Create result

	rows = make([][]any, len(logs))

	for i, l := range logs {
		row := make([]any, len(p.query.Select))

		for j, entry := range p.query.Select {
			if val, ok := l.GetValue(entry); ok {
				row[j] = val
			} else if aggr, ok := p.aggrs[entry]; ok {
				row[j] = aggr.GetResult()
			} else {
				row[j] = entry
			}
		}

		rows[i] = row
	}

	p.trace.STAGE(nil, "Results getted: ", len(rows), " rows")
	return rows, nil
}

// Groups

func (p *Processor) sortGroups(groups []*m.AggrSource) error {
	if len(groups) < 2 {
		return nil
	}
	key := p.query.OrderBy

	if key == "" {
		return nil
	}
	desk := true

	if key[0] == '-' {
		desk = false
		key = key[1:]
	}

	val, ok := groups[0].GetValue(key)

	if !ok {
		return errors.New("is not aggregator or grouping column: " + key)
	}

	switch val.(type) {

	case string:
		tools.SortWithKey(groups, func(g *m.AggrSource) string {
			val, _ := g.GetValue(key)
			return val.(string)
		}, desk)

	case int64:
		tools.SortWithKey(groups, func(g *m.AggrSource) int64 {
			val, _ := g.GetValue(key)
			return val.(int64)
		}, desk)

	case []string:
		tools.SortWithKey(groups, func(g *m.AggrSource) string {
			val, _ := g.GetValue(key)
			return tools.ToOrderedValue(val).(string)
		}, desk)

	default:
		return aerr.NewAppErr(aerr.BadReq, "cannot order by column or aggregator: ", key)
	}

	p.trace.DEBUG(nil, "Groups ordered by: ", p.query.OrderBy)
	return nil
}

func (p *Processor) getResultFromGroups() (rows [][]any, err error) {
	defer p.trace.AddModule("_Searcher", "getResultFromGroups")()
	p.trace.STAGE(nil, "Getting results...")

	groups, err := p.groups.GetAggrSources(p.trace)

	if err != nil {
		return nil, err
	}
	p.sortGroups(groups)

	// Get group slice

	if p.offset >= uint(len(groups)) {
		return [][]any{}, nil
	}
	end := int(p.offset + p.limit)

	if p.limit == 0 || end > len(groups) {
		end = len(groups)
	}
	groups = groups[p.offset:end]

	// Create result

	rows = make([][]any, len(groups))

	for i, group := range groups {
		row := make([]any, len(p.query.Select))

		for j, entry := range p.query.Select {
			if val, ok := group.GetValue(entry); ok {
				row[j] = val
			} else {
				row[j] = entry
			}
		}
		rows[i] = row
	}

	p.trace.STAGE(nil, "Results getted: ", len(rows), " rows")
	return rows, nil
}

// Public

func (p *Processor) PutLogs(logs []*m.Log) error {
	defer p.trace.AddModule("_Searcher", "PutLogs")()

	logPack := []*m.Log{}

	for _, l := range logs {
		// Filter

		if p.whereCond != nil {
			ok, err := p.whereCond.Check(p.trace, l)

			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		// Collect or/and pass through aggregators

		if p.query.GroupBy == "" {
			logPack = append(logPack, l)

			for _, aggr := range p.aggrs {
				aggr.Update(p.trace, l)
			}
		} else {
			p.groups.Update(p.trace, l)
		}
	}

	if len(logPack) > 0 {
		p.logPacks = append(p.logPacks, logPack)
	}

	p.trace.DEBUG(nil, "Logs putted: ", len(p.logPacks), " chunks readed")
	return nil
}

func (p *Processor) PutLogsFromChanel(output <-chan []*m.Log, errCh <-chan error) error {
	for logs := range output {
		err := p.PutLogs(logs)

		if err != nil {
			return err
		}
	}
	return <-errCh
}

func (p *Processor) GetResult() ([][]any, error) {
	if p.query.GroupBy == "" {
		return p.getResultFromLogs()
	}
	return p.getResultFromGroups()
}

func (s *Processor) Equals(other *Processor) bool {
	return tools.EqualSlicesBy(s.logPacks, other.logPacks, func(logPack1, logPack2 []*m.Log) bool {
		return tools.DeepEqualSlices(logPack1, logPack2)
	}) &&
		s.query.Equals(other.query) &&
		s.whereCond.Equals(other.whereCond) &&
		tools.DeepEqualMaps(s.aggrs, other.aggrs) &&
		s.groups.Equals(other.groups) &&
		s.offset == other.offset &&
		s.limit == other.limit
}
