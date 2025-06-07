package storage

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	sl "github.com/j-hitgate/sherlog"

	"main/agents/log_utils"
	m "main/models"
	fsr "main/relays/file_sys"
	"main/tools"
)

type Scheduler struct {
	sr       *Reader
	sw       *Writer
	sd       *Deleter
	config   m.SchedulerConfig
	fileSys  *fsr.FileSys
	selector *log_utils.Selector

	isRunnedAligner        bool
	isRunnedExpiredDeleter bool
	isRunnedRemover        bool
}

func NewScheduler(trace *sl.Trace, sr *Reader, sw *Writer, sd *Deleter, config m.SchedulerConfig) *Scheduler {
	return &Scheduler{
		sr:       sr,
		sw:       sw,
		sd:       sd,
		config:   config,
		fileSys:  &fsr.FileSys{},
		selector: &log_utils.Selector{},
	}
}

func (s *Scheduler) RunAligner(metasMap *MetasMap) {
	if s.isRunnedAligner {
		return
	}
	go s.aligner(metasMap)
	s.isRunnedAligner = true
}

func (s *Scheduler) aligner(metasMap *MetasMap) {
	trace := sl.NewTrace("scheduler_aligner")
	trace.SetEntity("aligner", uuid.New().String())
	id := &struct{}{}

	for {
		trace.INFO(nil, "Aligning chunks...")
		storages := metasMap.Storages()
		alignedCount := 0

		for i := range storages {
			// Get and lock chunks with crossed time ranges

			unreserve := metasMap.ReserveVersion(trace, id)
			crossedMetas := metasMap.GetFulledCrossedMetas(trace, storages[i])
			mxs := make([]*sync.Mutex, len(crossedMetas))

			for j, meta := range crossedMetas {
				mxs[j] = meta.Mx
				mxs[j].Lock()
			}

			crossedMetas = metasMap.GetLastVersionMetas(trace, storages[i], crossedMetas)

			if len(crossedMetas) < 2 {
				unreserve(trace)

				for _, mx := range mxs {
					mx.Unlock()
				}

				trace.DEBUG(nil, "No chunks aligned in storage: ", storages[i])
				continue
			}

			// Read and align chunks

			logPacks := make([][]*m.Log, len(crossedMetas))

			for j := range crossedMetas {
				logPacks[j] = s.sr.ReadChunk(trace, storages[i], crossedMetas[j], nil)
			}

			s.AlignChunks(logPacks)

			// Write aligned chunks with new version

			backuper := fsr.NewBackuper(trace, fmt.Sprintf("%s_%d", storages[i], crossedMetas[0].ID))

			chunkNames := make([]string, len(crossedMetas))

			for j, meta := range crossedMetas {
				chunkNames[j] = meta.Name()
				s.sw.WriteNewVersionChunk(trace, storages[i], meta, logPacks[j], backuper)
			}
			backuper.Cancel()

			// Set state

			metasMap.Update(&m.UpdateStateTask{
				Storage:   storages[i],
				ForUpdate: crossedMetas,
				Trace:     trace,
				Callback: func() {
					for _, mx := range mxs {
						mx.Unlock()
					}
				},
			})
			unreserve(trace)
			alignedCount += len(crossedMetas)

			trace.DEBUG(
				sl.Fields{"chunks": strings.Join(chunkNames, ", ")},
				len(crossedMetas), " chunks aligned in storage: ", storages[i],
			)
		}

		trace.INFO(nil, alignedCount, " chunks aligned")
		time.Sleep(s.config.AligningPeriod)
	}
}

func (*Scheduler) AlignChunks(logPacks [][]*m.Log) {
	logs := tools.JoinSlices(logPacks...)

	sort.SliceStable(logs, func(i, j int) bool {
		return logs[i].Timestamp < logs[j].Timestamp
	})

	size := len(logs) / len(logPacks)
	offset := 0

	for i := 0; i < len(logPacks)-1; i++ {
		logPacks[i] = logs[offset : offset+size]
		offset += size
	}
	logPacks[len(logPacks)-1] = logs[offset:]
}

func (s *Scheduler) RunExpiredDeleter(metasMap *MetasMap) {
	if s.isRunnedExpiredDeleter {
		return
	}
	go s.expiredDeleter(metasMap)
	s.isRunnedExpiredDeleter = true
}

func (s *Scheduler) expiredDeleter(metasMap *MetasMap) {
	trace := sl.NewTrace("scheduler_expiredDeleter")
	trace.SetEntity("expiredDeleter", uuid.New().String())

	for {
		trace.INFO(nil, "Deleting expired chunks...")
		storages := metasMap.Storages()
		delCount := 0

		for i := range storages {
			unreserve := metasMap.ReserveVersion(trace, trace)

			deadline := time.Now().Add(-s.config.LogsTTL).UnixMilli()
			expired := metasMap.GetExpired(trace, storages[i], deadline)

			if len(expired) == 0 {
				unreserve(trace)
				trace.DEBUG(nil, "No chunks are expired in storage: ", storages[i])
				continue
			}

			backuper := fsr.NewBackuper(trace, fmt.Sprintf("%s_%d", storages[i], expired[0].ID))

			for _, meta := range expired {
				meta.Mx.Lock()
				s.sd.MarkChunkAsDeleted(trace, storages[i], meta, backuper)
			}
			backuper.Cancel()

			metasMap.Update(&m.UpdateStateTask{
				Storage:   storages[i],
				ForUpdate: expired,
				Trace:     trace,
				Callback: func() {
					for _, meta := range expired {
						meta.Mx.Unlock()
					}
				},
			})
			unreserve(trace)

			delCount += len(expired)
			trace.DEBUG(nil, len(expired), " chunks deleted from storage: ", storages[i])
		}

		trace.INFO(nil, delCount, " expired chunks deleted")
		time.Sleep(s.config.DelExpiredPeriod)
	}
}

func (s *Scheduler) RunRemover(metasMap *MetasMap) {
	if s.isRunnedRemover {
		return
	}
	go s.remover(metasMap)
	s.isRunnedRemover = true
}

func (s *Scheduler) remover(metasMap *MetasMap) {
	trace := sl.NewTrace("scheduler_remover")
	trace.SetEntity("remover", uuid.New().String())

	for {
		time.Sleep(s.config.RmFilesPeriod)
		trace.INFO(nil, "Removing files/dirs...")

		names := metasMap.GetForRemove()

		if len(names) == 0 {
			trace.INFO(nil, "No files/dirs removed")
			continue
		}

		s.fileSys.AtomicRemove(trace, names...)
		trace.INFO(nil, len(names), " files/dirs removed")
	}
}
