package test_tools

import (
	"time"

	sl "github.com/j-hitgate/sherlog"

	m "main/models"
)

func SherlogInit() {
	sl.Init(sl.Config{
		SyncPrint:     true,
		NotShowTraces: true,
		NotShowEntity: true,
	}, nil)
}

func CreateLog() *m.Log {
	return &m.Log{
		Timestamp: time.Now().UnixMilli(),
		Level:     1,
		Traces:    []string{"trace1", "trace2"},
		Entity:    "entity",
		EntityID:  "12345",
		Message:   "message",
		Modules:   []string{"module1", "module2", "module3"},
		Labels:    []string{"label1", "label2", "label3"},
		Fields:    map[string]string{"key1": "val1", "key2": "val2"},
	}
}
