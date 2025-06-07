package models

import (
	"sync"

	sl "github.com/j-hitgate/sherlog"
)

// Write

type WriteLogsTask struct {
	Storage string
	Logs    []*Log
	ErrCh   chan error
	Trace   *sl.Trace
}

type WriteToChunkTask struct {
	ChunkPath string
	Meta      *Meta
	Logs      []*Log
	Wg        *sync.WaitGroup
	Traces    map[string]*sl.Trace
}

func NewWriteToChunkTask(trace *sl.Trace, chunkPath string, meta *Meta, logs []*Log) *WriteToChunkTask {
	return &WriteToChunkTask{
		ChunkPath: chunkPath,
		Meta:      meta,
		Logs:      logs,
		Wg:        &sync.WaitGroup{},
		Traces:    trace.ForkOnMap(GetLogColumns()...),
	}
}

// Read

type ReadLogsTask struct {
	Lld    *LoadLogsData
	LogsCh chan []*Log
	ErrCh  chan error
	Trace  *sl.Trace
}

type ReadChunkTask struct {
	Logs      []*Log
	ChunkPath string
	Wg        *sync.WaitGroup
	Traces    map[string]*sl.Trace
}

func NewReadChunkTask(trace *sl.Trace, chunkPath string, logsLen int) *ReadChunkTask {
	logs := make([]*Log, logsLen)

	for i := range logs {
		logs[i] = &Log{}
	}

	return &ReadChunkTask{
		Logs:      logs,
		ChunkPath: chunkPath,
		Wg:        &sync.WaitGroup{},
		Traces:    trace.ForkOnMap(GetLogColumns()...),
	}
}

// Delete

type DeleteLogsTask struct {
	ID        string
	Storage   string
	TimeRange TimeRange
	Condition ICondition
}

// Update state

type UpdateStateTask struct {
	Storage   string
	ForUpdate []*Meta
	ForAdd    []*Meta
	Trace     *sl.Trace
	Callback  func()
}
