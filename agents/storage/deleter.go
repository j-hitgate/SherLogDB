package storage

import (
	"fmt"
	"path"

	"github.com/google/uuid"
	sl "github.com/j-hitgate/sherlog"

	conds "main/agents/conditions"
	"main/agents/log_utils"
	"main/agents/time_range"
	aerr "main/app_errors"
	m "main/models"
	fsr "main/relays/file_sys"
)

type Deleter struct {
	sr       *Reader
	sw       *Writer
	isRunned bool
	fileSys  *fsr.FileSys
	selector *log_utils.Selector
}

func NewDeleter(sr *Reader, sw *Writer) *Deleter {
	return &Deleter{
		sr:       sr,
		sw:       sw,
		fileSys:  &fsr.FileSys{},
		selector: &log_utils.Selector{},
	}
}

func (d *Deleter) RunDeleter(queue <-chan *m.DeleteQuery, metasMap *MetasMap) {
	if d.isRunned {
		return
	}
	taskQueue := make(chan *m.DeleteLogsTask, 20)
	go d.queryReceiver(queue, taskQueue, metasMap)
	go d.deleter(taskQueue, metasMap)
	d.isRunned = true
}

func (d *Deleter) queryReceiver(queue <-chan *m.DeleteQuery, taskQueue chan<- *m.DeleteLogsTask, metasMap *MetasMap) {
	for query := range queue {
		query.ErrCh <- func() (err error) {
			defer query.Trace.AddModule("_Deleter", "queryReceiver")()
			query.Trace.STAGE(nil, "Creating delete task...")

			if !metasMap.Exists(query.Storage) {
				err = aerr.NewAppErr(aerr.NotFound, "Storage '", query.Storage, "' not exists")
				query.Trace.NOTE(nil, err.Error())
				return err
			}

			// Parse and validate query in same time

			var condition m.ICondition

			if query.Where != "" {
				condition, err = conds.ParseCondition(query.Trace, query.Where, query.WhereValues, nil, nil)

				if err != nil {
					return err
				}
			}

			var tr m.TimeRange

			if query.TimeRange != "" {
				trp := time_range.NewParser(query.Trace)
				tr, err = trp.Parse(query.TimeRange)

				if err != nil {
					return err
				}
			}

			// Save task if it is new

			if query.TaskID == "" {
				query.TaskID = uuid.New().String()
				name := path.Join(m.DIR_DELETE_TASKS, query.TaskID)
				d.fileSys.WriteFile(query.Trace, name, true, query)
			}

			// Send task to deleter

			task := &m.DeleteLogsTask{
				ID:        query.TaskID,
				Storage:   query.Storage,
				TimeRange: tr,
				Condition: condition,
			}
			taskQueue <- task

			query.Trace.STAGE(nil, "Delete task created. ID: ", task.ID)
			return nil
		}()
		close(query.ErrCh)
	}
}

func (d *Deleter) deleter(queue <-chan *m.DeleteLogsTask, metasMap *MetasMap) {
	trace := sl.NewTrace("deleter_" + uuid.New().String()[:13])

	for task := range queue {
		popModule := trace.AddModule("_Deleter", "deleter")

		func() {
			trace.STAGE(sl.Fields{"ID": task.ID}, "Running delete task...")

			defer metasMap.ReserveVersion(trace, d)(trace)
			metas := metasMap.GetInRange(trace, task.Storage, task.TimeRange)

			if metas == nil {
				trace.STAGE(nil, "Delete task is already compeled")
				return
			}

			if len(metas) == 0 {
				trace.STAGE(nil, "No logs deleted")
				return
			}

			backuper := fsr.NewBackuper(trace, fmt.Sprint("del_"+task.ID))

			for _, meta := range metas {
				meta.Mx.Lock()
				meta = metasMap.Find(trace, task.Storage, meta.ID)
				isDeleted := true

				if task.Condition != nil {
					isDeleted = d.DeleteByCondition(trace, task.Storage, meta, task.TimeRange, task.Condition, backuper)
				} else {
					d.DeleteByTimeRange(trace, task.Storage, meta, task.TimeRange, backuper)
				}
				backuper.Cancel()

				if isDeleted {
					metasMap.Update(&m.UpdateStateTask{
						Storage:   task.Storage,
						ForUpdate: []*m.Meta{meta},
						Trace:     trace,
						Callback:  func() { meta.Mx.Unlock() },
					})
				}
				trace.DEBUG(nil, "Chunk ", meta.ID, " done")
			}
		}()

		name := path.Join(m.DIR_DELETE_TASKS, task.ID)
		d.fileSys.Remove(trace, name)

		trace.STAGE(nil, "Delete task completed")
		popModule()
	}
}

func (d *Deleter) MarkChunkAsDeleted(trace *sl.Trace, storage string, meta *m.Meta, backuper *fsr.Backuper) {
	defer trace.AddModule("_Deleter", "MarkChunkAsDeleted")()

	metaPath := path.Join(m.DIR_STORAGES, storage, meta.Name(), "meta.new")

	backuper.AddForReplace(metaPath)
	backuper.Commit()

	meta.IsDeleted = true
	d.fileSys.WriteFile(trace, metaPath, false, meta)

	trace.DEBUG(nil, "Chunk marked as deleted: ", storage, "/", meta.Name())
}

func (d *Deleter) DeleteByTimeRange(trace *sl.Trace, storage string, meta *m.Meta, tr m.TimeRange, backuper *fsr.Backuper) {
	popModule := trace.AddModule("_Deleter", "DeleteByTimeRange")
	defer func() {
		trace.DEBUG(nil, "Logs from chunks deleted: ", storage, "/", meta.Name())
		popModule()
	}()

	if time_range.IsInside(tr, meta.TimeRange) {
		d.MarkChunkAsDeleted(trace, storage, meta, backuper)
		return
	}
	logs := d.sr.ReadChunk(trace, storage, meta, nil)
	logs = d.selector.GetLogsOutOfRange(trace, logs, tr, meta.Offsets == nil)
	d.sw.WriteNewVersionChunk(trace, storage, meta, logs, backuper)
}

func (d *Deleter) DeleteByCondition(trace *sl.Trace, storage string, meta *m.Meta, tr m.TimeRange, cond m.ICondition, backuper *fsr.Backuper) bool {
	defer trace.AddModule("_Deleter", "DeleteByCondition")()
	chunk := fmt.Sprint(storage, "/", meta.Name())

	// Read logs from chunk and get indices on time range

	logs := d.sr.ReadChunk(trace, storage, meta, nil)
	startInx, endInx := d.selector.GetIndicesOfRange(trace, logs, tr, meta.Offsets == nil)

	// Filter logs

	var err error
	filteredLogs := make([]*m.Log, 0, len(logs))
	ok := false

	for i := range logs {
		if i < startInx || i > endInx {
			ok = false
		} else {
			ok, err = cond.Check(trace, logs[i])

			if err != nil {
				trace.ERROR(nil, "Incorrect condition: ", err.Error())
				return false
			}
		}

		if !ok {
			filteredLogs = append(filteredLogs, logs[i])
		}
	}

	if len(filteredLogs) == len(logs) {
		trace.DEBUG(nil, "No logs deleted: ", chunk)
		return false
	}

	if len(filteredLogs) == 0 {
		d.MarkChunkAsDeleted(trace, storage, meta, backuper)
		trace.DEBUG(nil, "Chunk deleted: ", chunk)
		return true
	}

	d.sw.WriteNewVersionChunk(trace, storage, meta, filteredLogs, backuper)
	trace.DEBUG(nil, len(logs)-len(filteredLogs), "logs deleted: ", chunk)
	return true
}
