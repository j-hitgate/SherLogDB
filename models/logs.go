package models

import (
	sl "github.com/j-hitgate/sherlog"

	aerr "main/app_errors"
	"main/tools"
)

type Log struct {
	Timestamp int64             `json:"timestamp"`
	Level     byte              `json:"level"`
	Traces    []string          `json:"traces"`
	Entity    string            `json:"entity"`
	EntityID  string            `json:"entity_id"`
	Message   string            `json:"message"`
	Modules   []string          `json:"modules"`
	Labels    []string          `json:"labels"`
	Fields    map[string]string `json:"fields"`
}

func (l *Log) Validate(trace *sl.Trace) (err error) {
	popModule := trace.AddModule("_Log", "Validate")
	defer func() {
		if err != nil {
			trace.NOTE(nil, err.Error())
		}
		popModule()
	}()

	// Integers

	if l.Timestamp == 0 {
		err = aerr.NewAppErr(aerr.BadReq, "'timestamp' a required column")
		return err
	}

	if l.Level > 7 {
		err = aerr.NewAppErr(aerr.BadReq, "'level' must be in range 0-7")
		return err
	}

	// Strings

	if l.Entity == "" || len(l.Entity) > 50 {
		err = aerr.NewAppErr(aerr.BadReq, "Number of characters in 'entity' must be from 1 to 50")
		return err
	}

	if l.EntityID == "" || len(l.EntityID) > 50 {
		err = aerr.NewAppErr(aerr.BadReq, "Number of characters in 'entity_id' must be from 1 to 50")
		return err
	}

	if l.Message == "" || len(l.Message) > 255 {
		err = aerr.NewAppErr(aerr.BadReq, "Number of characters in 'message' must be from 1 to 255")
		return err
	}

	// Arrays

	if len(l.Traces) == 0 || len(l.Traces) > 20 {
		err = aerr.NewAppErr(aerr.BadReq, "Number of 'traces' must be from 1 to 20")
		return err
	}

	for _, trace := range l.Traces {
		if trace == "" || len(trace) > 50 {
			err = aerr.NewAppErr(aerr.BadReq, "Number of characters in the all 'traces' must be from 1 to 50")
			return err
		}
	}

	if len(l.Modules) == 0 || len(l.Modules) > 40 {
		err = aerr.NewAppErr(aerr.BadReq, "Number of 'modules' must be from 1 to 40")
		return err
	}

	for _, module := range l.Modules {
		if module == "" || len(module) > 50 {
			err = aerr.NewAppErr(aerr.BadReq, "Number of characters in the all 'modules' must be from 1 to 50")
			return err
		}
	}

	if len(l.Labels) > 20 {
		err = aerr.NewAppErr(aerr.BadReq, "Number of 'labels' is more than 20")
		return err
	}

	for _, label := range l.Labels {
		if label == "" || len(label) > 50 {
			err = aerr.NewAppErr(aerr.BadReq, "Number of characters in the all 'labels' must be from 1 to 50")
			return err
		}
	}

	// Map

	if len(l.Fields) > 20 {
		err = aerr.NewAppErr(aerr.BadReq, "Number of parameters in 'fields' is more than 20 records")
		return err
	}

	for key, val := range l.Fields {
		if key == "" || len(key) > 50 || val == "" || len(val) > 50 {
			err = aerr.NewAppErr(aerr.BadReq,
				"Number of characters in the all keys and values in 'fields' must be from 1 to 50",
			)
			return err
		}
	}

	return nil
}

func (l *Log) GetValue(column string) (any, bool) {
	switch column {
	case C_TIMESTAMP:
		return l.Timestamp, true
	case C_LEVEL:
		return int64(l.Level), true
	case C_TRACES:
		return l.Traces, true
	case C_ENTITY:
		return l.Entity, true
	case C_ENTITY_ID:
		return l.EntityID, true
	case C_MESSAGE:
		return l.Message, true
	case C_MODULES:
		return l.Modules, true
	case C_LABELS:
		return l.Labels, true
	case C_FIELDS:
		return l.Fields, true
	}
	return nil, false
}

func (l *Log) Get(column string) (any, bool) {
	switch column {
	case C_TIMESTAMP:
		return &l.Timestamp, true
	case C_LEVEL:
		return &l.Level, true
	case C_TRACES:
		return &l.Traces, true
	case C_ENTITY:
		return &l.Entity, true
	case C_ENTITY_ID:
		return &l.EntityID, true
	case C_MESSAGE:
		return &l.Message, true
	case C_MODULES:
		return &l.Modules, true
	case C_LABELS:
		return &l.Labels, true
	case C_FIELDS:
		return &l.Fields, true
	}
	return nil, false
}

func (l *Log) Equals(other *Log) bool {
	return l.Timestamp == other.Timestamp &&
		l.Level == other.Level &&
		tools.EqualSlices(l.Traces, other.Traces) &&
		l.Entity == other.Entity &&
		l.EntityID == other.EntityID &&
		l.Message == other.Message &&
		tools.EqualSlices(l.Modules, other.Modules) &&
		tools.EqualSlices(l.Labels, other.Labels) &&
		tools.EqualMaps(l.Fields, other.Fields)
}

// ---

type Logs struct {
	Storage string `json:"storage"`
	Logs    []*Log `json:"logs"`
}

func (ls *Logs) Validate(trace *sl.Trace) error {
	defer trace.AddModule("_Logs", "Validate")()

	if ls.Storage == "" || len(ls.Storage) > 200 {
		err := aerr.NewAppErr(aerr.BadReq, "Number of characters in 'storage' must be from 1 to 200")
		trace.NOTE(nil, err.Error())
		return err
	}

	if len(ls.Logs) == 0 {
		err := aerr.NewAppErr(aerr.BadReq, "'logs' not specified")
		trace.NOTE(nil, err.Error())
		return err
	}

	for _, l := range ls.Logs {
		err := l.Validate(trace)

		if err != nil {
			return err
		}
	}

	return nil
}
