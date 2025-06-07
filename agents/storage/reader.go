package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path"

	sl "github.com/j-hitgate/sherlog"
	"github.com/vmihailenco/msgpack/v5"

	"main/agents/log_utils"
	aerr "main/app_errors"
	m "main/models"
	"main/relays/file_sys"
)

type Reader struct {
	columnQueues map[string]chan *m.ReadChunkTask
	isRunned     bool
	fileSys      *file_sys.FileSys
	selector     *log_utils.Selector
}

func NewReader() *Reader {
	r := &Reader{
		columnQueues: map[string]chan *m.ReadChunkTask{},
		fileSys:      &file_sys.FileSys{},
		selector:     &log_utils.Selector{},
	}

	columns := m.GetLogColumns()

	for i := range columns {
		r.columnQueues[columns[i]] = make(chan *m.ReadChunkTask, 1)
		go r.columnReader(columns[i], r.columnQueues[columns[i]])
	}

	return r
}

func (r *Reader) RunReader(queue <-chan *m.ReadLogsTask, metasMap *MetasMap) {
	if r.isRunned {
		return
	}
	go r.reader(queue, metasMap)
	r.isRunned = true
}

func (r *Reader) reader(queue <-chan *m.ReadLogsTask, metasMap *MetasMap) {
	for task := range queue {
		err := func() error {
			lld, trace := task.Lld, task.Trace

			defer trace.AddModule("_Reader", "reader")()
			trace.STAGE(nil, "Reading logs from storage '", lld.Storage, "'...")

			defer metasMap.ReserveVersion(trace, r)(trace)
			metas := metasMap.GetInRange(trace, lld.Storage, lld.TimeRange)

			if metas == nil {
				err := aerr.NewAppErr(aerr.NotFound, "Storage '", lld.Storage, "' not exists")
				task.Trace.NOTE(nil, err.Error())
				return err
			}

			if len(metas) == 0 {
				return nil
			}

			for _, meta := range metas {
				logs := r.ReadChunk(trace, lld.Storage, meta, lld.Columns)
				logs = r.selector.GetLogsInRange(trace, logs, lld.TimeRange, meta.Offsets == nil)
				task.LogsCh <- logs
			}

			trace.STAGE(nil, "Logs readed")
			return nil
		}()
		close(task.LogsCh)
		task.ErrCh <- err
		close(task.ErrCh)
	}
}

func (r *Reader) ReadChunk(trace *sl.Trace, storage string, meta *m.Meta, columns map[string]bool) []*m.Log {
	defer trace.AddModule("_Reader", "ReadChunk")()

	name := path.Join(m.DIR_STORAGES, storage, meta.Name())
	task := m.NewReadChunkTask(trace, name, meta.LogsLen)

	if len(columns) > 0 {
		columns[m.C_TIMESTAMP] = true

		task.Wg.Add(len(columns))

		for column := range columns {
			r.columnQueues[column] <- task
		}

	} else {
		task.Wg.Add(len(r.columnQueues))

		for _, queue := range r.columnQueues {
			queue <- task
		}
	}
	task.Wg.Wait()
	sl.CloseTraces(task.Traces)

	trace.DEBUG(nil, len(task.Logs), " logs readed from chunk: ", storage, "/", meta.Name())
	return task.Logs
}

func (*Reader) getLine(data []byte, i int) ([]byte, int, error) {
	if len(data) == 0 {
		return []byte{}, i, nil
	}

	lenByte := data[i : i+2]
	length := int(binary.LittleEndian.Uint16(lenByte))
	i += 2

	if i+length > len(data) {
		return nil, 0, errors.New(fmt.Sprint("Incorrect line length: ", length))
	}

	return data[i : i+length], i + length, nil
}

func (r *Reader) columnReader(column string, queue <-chan *m.ReadChunkTask) {
	for task := range queue {
		trace := task.Traces[column]

		name := path.Join(task.ChunkPath, column)
		data := r.fileSys.ReadFile(trace, name)

		var err error
		var line []byte
		j := 0

		for i := range task.Logs {
			line, j, err = r.getLine(data, j)

			if err != nil {
				fields := sl.Fields{"name": name, "column": column}
				trace.FATAL(fields, err.Error())
			}

			field, _ := task.Logs[i].Get(column)
			err := msgpack.Unmarshal(line, field)

			if err != nil {
				fields := sl.Fields{"name": name, "column": column}
				trace.FATAL(fields, "Convert from bytes error: ", err.Error())
			}
		}

		trace.DEBUG(nil, "Logs from column readed")
		task.Wg.Done()
	}
}
