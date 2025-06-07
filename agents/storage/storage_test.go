package storage

import (
	"fmt"
	"os"
	"path"
	"sort"
	"testing"

	sl "github.com/j-hitgate/sherlog"
	"github.com/stretchr/testify/assert"

	"main/agents/conditions"
	m "main/models"
	fsr "main/relays/file_sys"
	tt "main/test_tools"
	"main/tools"
)

func TestWriterAndReader(t *testing.T) {
	os.Chdir("../..")
	tt.SherlogInit()
	trace := sl.NewTrace("Main")

	// Create storage and logs

	storagePath := path.Join(m.DIR_STORAGES, "storage")
	os.MkdirAll(storagePath, 0755)
	defer os.RemoveAll(m.DIR_TRANSACTIONS)
	defer os.RemoveAll(m.DIR_STORAGES)

	logs := make([]*m.Log, 8)

	timestamps := []int64{
		9, 5, 7,
		3, 8, 1,
		4, 2,
	}

	for i := range logs {
		logs[i] = tt.CreateLog()
		logs[i].Timestamp = timestamps[i]
	}

	maxLogsInChunk := 3

	metasMap := NewMetasMap(100)
	metasMap.AddStorage(trace, "storage", []*m.Meta{})

	// Write logs

	sw := NewWriter(maxLogsInChunk)
	writeQueue := make(chan *m.WriteLogsTask, 1)
	sw.RunWriter(writeQueue, 0, map[string]uint64{"storage": 1}, 1, metasMap)

	writeTask := &m.WriteLogsTask{
		Storage: "storage",
		Logs:    logs,
		ErrCh:   make(chan error, 1),
		Trace:   trace,
	}
	writeQueue <- writeTask
	err := <-writeTask.ErrCh

	if err != nil {
		assert.Fail(t, err.Error())
		return
	}

	// Read logs

	sr := NewReader()
	readQueue := make(chan *m.ReadLogsTask, 1)
	sr.RunReader(readQueue, metasMap)

	readTask := &m.ReadLogsTask{
		Lld:    &m.LoadLogsData{Storage: "storage"},
		LogsCh: make(chan []*m.Log, 1),
		ErrCh:  make(chan error, 1),
		Trace:  trace,
	}
	readQueue <- readTask

	readedLogs := make([]*m.Log, 0, len(logs))
	chunk := 1

	for logPack := range readTask.LogsCh {
		if (chunk < 3 && len(logPack) != maxLogsInChunk) || (chunk == 3 && len(logPack) != 2) {
			assert.Fail(t, "Chunk =", chunk, ", len =", len(logPack))
			return
		}
		readedLogs = append(readedLogs, logPack...)
		chunk++
	}
	err = <-readTask.ErrCh

	if err != nil {
		assert.Fail(t, err.Error())
		return
	}

	// Equals

	timestamps = []int64{
		5, 7, 9,
		1, 3, 8,
		4, 2,
	}

	for i := range readedLogs {
		if !assert.Equal(t, timestamps[i], readedLogs[i].Timestamp, fmt.Sprint("Log ", i)) {
			return
		}
	}
}

func TestDeleter(t *testing.T) {
	os.Chdir("../..")
	tt.SherlogInit()
	trace := sl.NewTrace("Main")

	// Create storage and logs

	storagePath := path.Join(m.DIR_STORAGES, "storage")
	os.MkdirAll(storagePath, 0755)
	defer os.RemoveAll(m.DIR_TRANSACTIONS)
	defer os.RemoveAll(m.DIR_STORAGES)

	logData := []struct {
		timestamp int64
		level     byte
		entity    string
	}{
		{timestamp: 1, level: 1, entity: "entity1"},
		{timestamp: 2, level: 1, entity: "entity2"},
		{timestamp: 3, level: 2, entity: "entity3"},
		{timestamp: 4, level: 2, entity: "entity4"},

		{timestamp: 5, level: 1, entity: "entity5"},
		{timestamp: 6, level: 2, entity: "entity6"},
		{timestamp: 7, level: 3, entity: "entity7"},
		{timestamp: 8, level: 4, entity: "entity8"},

		{timestamp: 9, level: 4, entity: "entity9"},
		{timestamp: 10, level: 5, entity: "entity10"},
		{timestamp: 11, level: 4, entity: "entity11"},
		{timestamp: 12, level: 5, entity: "entity12"},
	}

	logs := make([]*m.Log, len(logData))

	for i := range logs {
		logs[i] = tt.CreateLog()
		logs[i].Timestamp = logData[i].timestamp
		logs[i].Level = logData[i].level
		logs[i].Entity = logData[i].entity
	}

	// Save logs

	metas := []*m.Meta{
		{ID: 1, Version: 1, TimeRange: m.TimeRange{Start: 1, End: 4}},
		{ID: 2, Version: 1, TimeRange: m.TimeRange{Start: 5, End: 8}},
		{ID: 3, Version: 1, TimeRange: m.TimeRange{Start: 9, End: 12}},
	}

	backuper := fsr.NewBackuper(trace, "storage_1")
	sw := NewWriter(4)
	j := 0

	for i := range metas {
		j = i * 4
		sw.WriteToChunk(trace, "storage", metas[i], logs[j:j+4], backuper)
	}
	backuper.Cancel()

	// Delete logs

	sr := NewReader()
	sd := NewDeleter(sr, sw)

	// Mark as deleted

	sd.MarkChunkAsDeleted(trace, "storage", metas[0], backuper)
	backuper.Cancel()

	meta := &m.Meta{}
	sr.fileSys.ReadFileTo(trace, path.Join(m.DIR_STORAGES, "storage", metas[0].Name(), "meta"), meta)

	if !meta.IsDeleted {
		assert.Fail(t, "Chunk not marked as deleted")
		return
	}

	// Delete by time range

	tr := m.TimeRange{Start: 6, End: 7}

	sd.DeleteByTimeRange(trace, "storage", metas[1], tr, backuper)
	backuper.Cancel()

	logPack := sr.ReadChunk(trace, "storage", metas[1], nil)

	if metas[1].Version != 2 || len(logPack) != 2 || logPack[0].Level != 1 || logPack[1].Level != 4 {
		assert.Fail(t, "Delete by time range -> Chunk: ", metas[1].Name(), ", Len: ", len(logPack))
		return
	}

	// Delete by condition

	cond, err := conditions.ParseCondition(trace, "level == ?0", []any{5.0}, nil, nil)

	if err != nil {
		assert.Fail(t, "Parse condition error: ", err.Error())
		return
	}

	tr.Start = 11
	tr.End = 0

	sd.DeleteByCondition(trace, "storage", metas[2], tr, cond, backuper)
	backuper.Cancel()

	logPack = sr.ReadChunk(trace, "storage", metas[2], nil)

	if metas[2].Version != 2 || len(logPack) != 3 || logPack[0].Level != 4 || logPack[1].Level != 5 || logPack[2].Level != 4 {
		assert.Fail(t, "Delete by time range -> Chunk: ", metas[1].Name(), ", Len: ", len(logPack))
	}
}

func TestAlignChunks(t *testing.T) {
	s := &Scheduler{}

	logPacks := [][]*m.Log{
		{
			{Timestamp: 1},
			{Timestamp: 3},
			{Timestamp: 9},
		},
		{
			{Timestamp: 2},
			{Timestamp: 4},
			{Timestamp: 6},
			{Timestamp: 8},
			{Timestamp: 10},
		},
		{
			{Timestamp: 5},
			{Timestamp: 7},
		},
	}

	s.AlignChunks(logPacks)

	logs := tools.JoinSlices(logPacks...)

	assert.True(t, sort.SliceIsSorted(logs, func(i, j int) bool {
		return logs[i].Timestamp < logs[j].Timestamp
	}))
}
