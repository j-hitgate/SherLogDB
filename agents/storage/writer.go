package storage

import (
	"encoding/binary"
	"fmt"
	"path"
	"sort"
	"sync"

	sl "github.com/j-hitgate/sherlog"
	"github.com/vmihailenco/msgpack/v5"

	aerr "main/app_errors"
	m "main/models"
	fsr "main/relays/file_sys"
	"main/tools"
)

type Writer struct {
	maxLogsInChunk int
	columnQueues   []chan *m.WriteToChunkTask
	isRunned       bool
	fileSys        *fsr.FileSys
}

func NewWriter(maxLogsInChunk int) *Writer {
	columns := m.GetLogColumns()

	w := &Writer{
		maxLogsInChunk: maxLogsInChunk,
		columnQueues:   make([]chan *m.WriteToChunkTask, len(columns)),
		fileSys:        &fsr.FileSys{},
	}

	for i := range columns {
		w.columnQueues[i] = make(chan *m.WriteToChunkTask, 1)
		go w.columnWriter(columns[i], w.columnQueues[i])
	}

	return w
}

func (w *Writer) RunWriter(queue <-chan *m.WriteLogsTask, instanceNum uint64, firstRawChunks map[string]uint64, step uint64, metasMap *MetasMap) {
	if w.isRunned {
		return
	}
	chunksForWrite := map[string]uint64{}

	for storage, id := range firstRawChunks {
		chunksForWrite[storage] = id + instanceNum
	}
	go w.writer(queue, instanceNum, chunksForWrite, step, metasMap)
	w.isRunned = true
}

func (w *Writer) writer(queue <-chan *m.WriteLogsTask, instanceNum uint64, chunksForWrite map[string]uint64, step uint64, metasMap *MetasMap) {
	sr := NewReader()
	waitUpdates := &sync.WaitGroup{}

	for task := range queue {
		waitUpdates.Wait()

		task.ErrCh <- func() error {
			trace := task.Trace

			defer trace.AddModule("_Writer", "writer")()
			trace.STAGE(nil, "Writing ", len(task.Logs), " logs to storage '", task.Storage, "'...")

			defer metasMap.ReserveVersion(trace, w)(trace)
			forUpdate, forAdd := []*m.Meta{}, []*m.Meta{}

			// Get chunk id

			id, ok := chunksForWrite[task.Storage]

			if !metasMap.Exists(task.Storage) {
				if ok {
					delete(chunksForWrite, task.Storage)
				}
				err := aerr.NewAppErr(aerr.NotFound, "Storage '", task.Storage, "' not exists")
				trace.NOTE(nil, err.Error())
				return err
			}

			if !ok {
				id = 1 + instanceNum
				chunksForWrite[task.Storage] = id
			}

			// Write logs

			backuper := fsr.NewBackuper(trace, fmt.Sprintf("%s_%d", task.Storage, id))
			writed := 0

			for writed < len(task.Logs) {
				meta := metasMap.Find(trace, task.Storage, id)

				if meta != nil {
					meta.Mx.Lock()
					meta = metasMap.Find(trace, task.Storage, id)
					forUpdate = append(forUpdate, meta)
				} else {
					meta = m.NewMeta(id, task.Logs[0].Timestamp)
					forAdd = append(forAdd, meta)
				}

				totalLogs := meta.LogsLen + len(task.Logs) - writed
				var logs []*m.Log

				if totalLogs < w.maxLogsInChunk {
					logs = task.Logs[writed:]

				} else {
					logs = sr.ReadChunk(trace, task.Storage, meta, nil)

					willWritten := w.maxLogsInChunk - meta.LogsLen
					logs = tools.JoinSlices(logs, task.Logs[writed:writed+willWritten])

					sort.Slice(logs, func(i, j int) bool {
						return logs[i].Timestamp < logs[j].Timestamp
					})

					meta.Version++
					meta.LogsLen = 0

					id += step
				}

				writed += w.WriteToChunk(trace, task.Storage, meta, logs, backuper)
			}

			backuper.Cancel()
			waitUpdates.Add(1)

			metasMap.Update(&m.UpdateStateTask{
				Storage:   task.Storage,
				ForUpdate: forUpdate,
				ForAdd:    forAdd,
				Trace:     trace,
				Callback: func() {
					for _, meta := range forUpdate {
						meta.Mx.Unlock()
					}
					waitUpdates.Done()
				},
			})
			chunksForWrite[task.Storage] = id

			trace.STAGE(nil, "Logs writed")
			return nil
		}()
		close(task.ErrCh)
	}
}

func (w *Writer) WriteNewVersionChunk(trace *sl.Trace, storage string, meta *m.Meta, logs []*m.Log, backuper *fsr.Backuper) (writed int) {
	meta.Version++
	meta.LogsLen = 0

	if meta.Offsets == nil {
		meta.TimeRange.Start = logs[0].Timestamp
		meta.TimeRange.End = logs[len(logs)-1].Timestamp
	} else {
		meta.Offsets = &m.Offsets{}
		meta.TimeRange.Start = logs[0].Timestamp
		meta.TimeRange.End = logs[0].Timestamp
	}

	return w.WriteToChunk(trace, storage, meta, logs, backuper)
}

func (w *Writer) WriteToChunk(trace *sl.Trace, storage string, meta *m.Meta, logs []*m.Log, backuper *fsr.Backuper) (writed int) {
	defer trace.AddModule("_Writer", "WriteToChunk")()

	// Calculate logs to append

	if len(logs) == 0 {
		trace.WARN(nil, "No logs to write")
		return 0
	}

	free := w.maxLogsInChunk - meta.LogsLen
	willWritten := tools.Min(free, len(logs))

	if willWritten == 0 {
		trace.WARN(nil, "No logs written")
		return 0
	}

	meta.LogsLen += willWritten

	if meta.LogsLen == w.maxLogsInChunk {
		meta.Offsets = nil
	}

	// Append values from logs to columns

	name := path.Join(m.DIR_STORAGES, storage, meta.Name())

	backuper.AddChunk(name, meta.Offsets)
	backuper.Commit()

	task := m.NewWriteToChunkTask(trace, name, meta, logs[:willWritten])
	task.Wg.Add(len(w.columnQueues))

	for i := range w.columnQueues {
		w.columnQueues[i] <- task
	}
	task.Wg.Wait()
	sl.CloseTraces(task.Traces)

	// Save meta

	name = path.Join(name, "meta.new")
	w.fileSys.WriteFile(trace, name, false, meta)

	trace.DEBUG(nil, willWritten, "/", len(logs), " logs written in chunk: ", storage, "/", meta.Name())
	return willWritten
}

func (*Writer) lenToBytes(data []byte) []byte {
	lenByte := make([]byte, 2)
	binary.LittleEndian.PutUint16(lenByte, uint16(len(data)))
	return lenByte
}

func (w *Writer) columnWriter(column string, queue <-chan *m.WriteToChunkTask) {
	for task := range queue {
		trace := task.Traces[column]

		trace.WithModule("_Writer", "columnWriter_"+column, func() {
			meta := task.Meta
			buffs := make([][]byte, 0, len(task.Logs)*2)

			for i := range task.Logs {
				val, _ := task.Logs[i].Get(column)
				data, err := msgpack.Marshal(val)

				if err != nil {
					trace.FATAL(nil, "Column '", column, "' of log not converting in bytes: ", err.Error())
				}

				buffs = append(buffs, w.lenToBytes(data), data)

				if column == m.C_TIMESTAMP {
					ts := *val.(*int64)

					if ts < meta.TimeRange.Start {
						meta.TimeRange.Start = ts

					} else if meta.TimeRange.End < ts {
						meta.TimeRange.End = ts
					}
				}
			}

			name := path.Join(task.ChunkPath, column)
			n := w.fileSys.AppendFile(trace, name, buffs)

			if meta.Offsets != nil {
				offset, _ := meta.Offsets.Get(column)
				*offset += int64(n)
			}

			trace.DEBUG(nil, "Logs in column written")
		})

		task.Wg.Done()
	}
}
