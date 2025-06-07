package models

type Offsets struct {
	Timestamp int64
	Level     int64
	Traces    int64
	Entity    int64
	EntityID  int64
	Message   int64
	Modules   int64
	Labels    int64
	Fields    int64
}

func (o *Offsets) Get(column string) (*int64, bool) {
	switch column {
	case C_TIMESTAMP:
		return &o.Timestamp, true
	case C_LEVEL:
		return &o.Level, true
	case C_TRACES:
		return &o.Traces, true
	case C_ENTITY:
		return &o.Entity, true
	case C_ENTITY_ID:
		return &o.EntityID, true
	case C_MESSAGE:
		return &o.Message, true
	case C_MODULES:
		return &o.Modules, true
	case C_LABELS:
		return &o.Labels, true
	case C_FIELDS:
		return &o.Fields, true
	default:
		return nil, false
	}
}

func (o *Offsets) IsZero() bool {
	return o.Timestamp == 0
}

func (o *Offsets) Equals(other *Offsets) bool {
	return *o == *other
}
