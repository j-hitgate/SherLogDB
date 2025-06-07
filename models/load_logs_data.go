package models

import (
	"main/tools"
)

type LoadLogsData struct {
	Storage   string
	Columns   map[string]bool
	TimeRange TimeRange
}

func NewLoadLogsData() *LoadLogsData {
	return &LoadLogsData{
		Columns: map[string]bool{},
	}
}

func (lld *LoadLogsData) Equals(other *LoadLogsData) bool {
	return lld.Storage == other.Storage &&
		tools.EqualMaps(lld.Columns, other.Columns) &&
		lld.TimeRange == other.TimeRange
}
