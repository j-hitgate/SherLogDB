package models

import (
	sl "github.com/j-hitgate/sherlog"

	aerr "main/app_errors"
	"main/tools"
)

// Search logs

type SearchQuery struct {
	Storage      string   `json:"storage"`
	Select       []string `json:"select"`
	TimeRange    string   `json:"time_range"`
	AggregValues []any    `json:"aggreg_values"`
	Where        string   `json:"where"`
	WhereValues  []any    `json:"where_values"`
	GroupBy      string   `json:"group_by"`
	Having       string   `json:"having"`
	HavingValues []any    `json:"having_values"`
	OrderBy      string   `json:"order_by"`
	Limit        uint     `json:"limit"`
	Offset       uint     `json:"offset"`
}

func (q *SearchQuery) Equals(other *SearchQuery) bool {
	return q.Storage == other.Storage &&
		tools.EqualSlices(q.Select, other.Select) &&
		tools.EqualSlices(q.AggregValues, other.AggregValues) &&
		q.Where == other.Where &&
		tools.EqualSlices(q.WhereValues, other.WhereValues) &&
		q.GroupBy == other.GroupBy &&
		q.Having == other.Having &&
		tools.EqualSlices(q.HavingValues, other.HavingValues) &&
		q.OrderBy == other.OrderBy &&
		q.Limit == other.Limit &&
		q.Offset == other.Offset
}

// Delete logs

type DeleteQuery struct {
	Storage     string     `json:"storage"`
	TimeRange   string     `json:"time_range"`
	Where       string     `json:"where"`
	WhereValues []any      `json:"where_values"`
	TaskID      string     `json:"-" msgpack:"-"`
	ErrCh       chan error `json:"-" msgpack:"-"`
	Trace       *sl.Trace  `json:"-" msgpack:"-"`
}

func (dl *DeleteQuery) Validate(trace *sl.Trace) error {
	defer trace.AddModule("_DeleteQuery", "Validate")()

	if dl.Storage == "" || len(dl.Storage) > 200 {
		err := aerr.NewAppErr(aerr.BadReq, "Number of characters in 'storage' must be from 1 to 200")
		trace.NOTE(nil, err.Error())
		return err
	}
	return nil
}

// Storage

type Storage struct {
	Storage string `json:"storage"`
}

func (s *Storage) Validate(trace *sl.Trace) error {
	defer trace.AddModule("_Storage", "Validate")()

	if s.Storage == "" || len(s.Storage) > 200 {
		err := aerr.NewAppErr(aerr.BadReq, "Number of characters in 'storage' must be from 1 to 200")
		trace.NOTE(nil, err.Error())
		return err
	}

	return nil
}

// Shutdown

type Shutdown struct {
	Password string `json:"password"`
}
