package storage

import (
	"fmt"
	"math"
	"path"
	"sort"
	"sync"

	sl "github.com/j-hitgate/sherlog"

	"main/agents/time_range"
	m "main/models"
)

type stateManager struct {
	metasMap map[string][][]*m.Meta
	version  uint64
	mx       *sync.Mutex
}

func newStateManager() *stateManager {
	return &stateManager{
		metasMap: map[string][][]*m.Meta{},
		version:  1,
		mx:       &sync.Mutex{},
	}
}

func (sm *stateManager) Get(storage string) (blocks [][]*m.Meta, version uint64) {
	sm.mx.Lock()

	if ms, ok := sm.metasMap[storage]; ok {
		blocks, version = ms, sm.version
	}
	sm.mx.Unlock()

	return blocks, version
}

func (sm *stateManager) Add(storage string, blocks [][]*m.Meta) (version uint64) {
	sm.mx.Lock()

	if _, ok := sm.metasMap[storage]; !ok {
		sm.metasMap[storage] = blocks
		sm.version++
		version = sm.version
	}
	sm.mx.Unlock()
	return version
}

func (sm *stateManager) Set(storage string, blocks [][]*m.Meta) (version uint64) {
	sm.mx.Lock()

	if _, ok := sm.metasMap[storage]; ok {
		sm.version++
		sm.metasMap[storage], version = blocks, sm.version
	}
	sm.mx.Unlock()
	return version
}

func (sm *stateManager) Delete(storage string) (version uint64) {
	sm.mx.Lock()

	if _, ok := sm.metasMap[storage]; ok {
		delete(sm.metasMap, storage)
		sm.version++
		version = sm.version
	}
	sm.mx.Unlock()
	return version
}

func (sm *stateManager) Version() uint64 {
	sm.mx.Lock()
	version := sm.version
	sm.mx.Unlock()
	return version
}

func (sm *stateManager) Storages() (storages []string, version uint64) {
	sm.mx.Lock()
	storages = make([]string, len(sm.metasMap))
	i := 0

	for storage := range sm.metasMap {
		storages[i] = storage
		i++
	}
	version = sm.version
	sm.mx.Unlock()

	return storages, version
}

func (sm *stateManager) Exists(storage string) bool {
	sm.mx.Lock()
	_, ok := sm.metasMap[storage]
	sm.mx.Unlock()
	return ok
}

type MetasMap struct {
	state        *stateManager
	queue        chan *m.UpdateStateTask
	deleteList   map[string]uint64
	uses         map[any]uint64
	usesMx       *sync.Mutex
	deletedCount int
	blockMaxSize int
}

func NewMetasMap(blockMaxSize int) *MetasMap {
	mm := &MetasMap{
		state:        newStateManager(),
		queue:        make(chan *m.UpdateStateTask, 20),
		deleteList:   map[string]uint64{},
		uses:         map[any]uint64{},
		usesMx:       &sync.Mutex{},
		blockMaxSize: blockMaxSize,
	}
	go mm.updateHandler()
	return mm
}

func (*MetasMap) binSearchBlock(blocks [][]*m.Meta, id uint64) int {
	if len(blocks) == 0 {
		return -1
	}
	lastBlock := blocks[len(blocks)-1]

	if id < blocks[0][0].ID || lastBlock[len(lastBlock)-1].ID < id {
		return -1
	}

	l, r, i := 0, len(blocks), 0

	for l < r {
		i = (l + r) / 2

		if blocks[i][0].ID <= id && id <= blocks[i][len(blocks[i])-1].ID {
			return i
		}
		if id < blocks[i][0].ID {
			r = i
		} else {
			l = i + 1
		}
	}

	return -1
}

func (*MetasMap) binSearchMeta(metas []*m.Meta, id uint64) int {
	if len(metas) == 0 || id < metas[0].ID || metas[len(metas)-1].ID < id {
		return -1
	}

	l, r, i := 0, len(metas), 0

	for l < r {
		i = (l + r) / 2

		if metas[i].ID == id {
			return i
		}
		if metas[i].ID < id {
			l = i + 1
		} else {
			r = i
		}
	}

	return -1
}

func (mm *MetasMap) calcIndices(index int) (blockIdx int, metaIdx int) {
	blockIdx = index / mm.blockMaxSize
	metaIdx = index - blockIdx*mm.blockMaxSize
	return blockIdx, metaIdx
}

func (mm *MetasMap) metasLen(blocks [][]*m.Meta) int {
	if len(blocks) == 0 {
		return 0
	}
	return (len(blocks)-1)*mm.blockMaxSize + len(blocks[len(blocks)-1])
}

// Deleter

func (mm *MetasMap) ReserveVersion(trace *sl.Trace, user any) (unreserve func(*sl.Trace)) {
	defer trace.AddModule("_MetasMap", "ReserveVersion")()

	mm.usesMx.Lock()
	version := mm.state.Version()
	mm.uses[user] = version
	mm.usesMx.Unlock()

	trace.DEBUG(nil, "Locked version ", version)

	return func(trace *sl.Trace) {
		defer trace.AddModule("_MetasMap", "ReserveVersion_unlock")()

		mm.usesMx.Lock()
		delete(mm.uses, user)
		mm.usesMx.Unlock()

		trace.DEBUG(nil, "Unlocked version ", version)
	}
}

func (mm *MetasMap) GetForRemove() []string {
	forDelete := []string{}
	mm.usesMx.Lock()

	if len(mm.deleteList) == 0 {
		mm.usesMx.Unlock()
		return forDelete
	}

	var minVer uint64 = math.MaxUint64

	for _, version := range mm.uses {
		if version < minVer {
			minVer = version
		}
	}

	for name, version := range mm.deleteList {
		if minVer >= version {
			forDelete = append(forDelete, name)
		}
	}

	for i := range forDelete {
		delete(mm.deleteList, forDelete[i])
	}

	mm.usesMx.Unlock()
	return forDelete
}

// Storages

func (mm *MetasMap) Storages() []string {
	storages, _ := mm.state.Storages()
	return storages
}

func (mm *MetasMap) Exists(storage string) bool {
	return mm.state.Exists(storage)
}

func (mm *MetasMap) AddStorage(trace *sl.Trace, storage string, metas []*m.Meta) bool {
	defer trace.AddModule("_MetasMap", "AddStorage")()

	blocks := [][]*m.Meta{}

	if len(metas) > 0 {
		i, _ := mm.calcIndices(len(metas) - 1)
		blocks = make([][]*m.Meta, i+1)

		for i := range blocks {
			blocks[i] = make([]*m.Meta, 0, mm.blockMaxSize)
		}
		i = 0

		for _, meta := range metas {
			blocks[i] = append(blocks[i], meta.Copy())

			if len(blocks[i]) == mm.blockMaxSize {
				i++
			}
		}
	}

	version := mm.state.Add(storage, blocks)

	if version > 0 {
		trace.DEBUG(nil, "Added storage: ", storage)
	} else {
		trace.DEBUG(nil, "Storage already exists: ", storage)
	}
	return version > 0
}

func (mm *MetasMap) DeleteStorage(trace *sl.Trace, storage string) bool {
	defer trace.AddModule("_MetasMap", "DeleteStorage")()
	version := mm.state.Delete(storage)

	if version == 0 {
		trace.DEBUG(nil, "Storage not exists: ", storage)
		return false
	}

	mm.usesMx.Lock()
	name := path.Join(m.DIR_STORAGES, storage)
	mm.deleteList[name] = version
	mm.usesMx.Unlock()

	trace.DEBUG(nil, "Storage deleted: ", storage)
	return true
}

// Update metas

func (mm *MetasMap) Update(task *m.UpdateStateTask) {
	mm.queue <- task
}

func (mm *MetasMap) updateAndReconstruct(newBlocks, blocks [][]*m.Meta, task *m.UpdateStateTask) (index int, deleted []*m.Meta) {
	forUpd := task.ForUpdate
	deleted = []*m.Meta{}
	i, j, k := 0, 0, 0

	newBlocks[0] = make([]*m.Meta, mm.blockMaxSize)

	for i_ := range blocks {
		for _, meta := range blocks[i_] {
			if j == mm.blockMaxSize {
				i++
				j = 0
				newBlocks[i] = make([]*m.Meta, mm.blockMaxSize)
			}

			if meta.IsDeleted {
				continue
			}

			if k < len(forUpd) && meta.ID == forUpd[k].ID {
				if forUpd[k].Version > meta.Version || forUpd[k].IsDeleted {
					deleted = append(deleted, meta)

					if forUpd[k].IsDeleted {
						k++
						continue
					}
				}
				newBlocks[i][j] = forUpd[k].Copy()
				k++

			} else {
				newBlocks[i][j] = meta
			}
			j++
		}
	}
	newBlocks[i] = newBlocks[i][:j]

	mm.deletedCount = 0
	return i*mm.blockMaxSize + j, deleted
}

func (mm *MetasMap) update(newBlocks, blocks [][]*m.Meta, task *m.UpdateStateTask) (index int, deleted []*m.Meta) {
	copy(newBlocks, blocks)

	forUpd := task.ForUpdate
	deleted = []*m.Meta{}
	i, j := 0, 0

	prevBlockIdx := -1
	var block []*m.Meta

	for k := range forUpd {
		i = mm.binSearchBlock(blocks, forUpd[k].ID)

		if i == -1 {
			continue
		}
		j = mm.binSearchMeta(blocks[i], forUpd[k].ID)

		if j == -1 {
			continue
		}

		// Copy block for update
		if i > prevBlockIdx {
			block = make([]*m.Meta, len(blocks[i]))
			copy(block, blocks[i])
			newBlocks[i] = block
			prevBlockIdx = i
		}

		if forUpd[k].Version > block[j].Version || forUpd[k].IsDeleted {
			deleted = append(deleted, block[j])

			if forUpd[k].IsDeleted {
				mm.deletedCount++
			}
		}
		block[j] = forUpd[k].Copy()
	}

	return mm.metasLen(blocks), deleted
}

func (mm *MetasMap) updateHandler() {
	for task := range mm.queue {
		task.Trace.WithModule("_MetasMap", "updateHandler", func() {
			fields := sl.WithFields(
				"storage", task.Storage,
				"forUpdate", len(task.ForUpdate),
				"forAdd", len(task.ForAdd),
			)

			blocks, version := mm.state.Get(task.Storage)

			if version == 0 {
				task.Trace.DEBUG(fields, "Storage '", task.Storage, "' not exists")
				return
			}

			if len(blocks) == 0 && len(task.ForAdd) == 0 {
				task.Trace.DEBUG(fields, "No metas to update")
				return
			}

			// Calculate and allocate empty cell for new blocks

			newLen := mm.metasLen(blocks) + len(task.ForAdd)
			lastBlockIdx, _ := mm.calcIndices(newLen - 1)
			newBlocks := make([][]*m.Meta, lastBlockIdx+1)

			// Set exist blocks in newBlocks and update them

			var deleted []*m.Meta
			index := 0

			if len(blocks) > 0 {
				sort.Slice(task.ForUpdate, func(i, j int) bool {
					return task.ForUpdate[i].ID < task.ForUpdate[j].ID
				})

				// If the number of deleted metas is more than 10%
				if mm.deletedCount > 0 && float64(newLen)/float64(mm.deletedCount) > 0.1 {
					// Slower, but gets rid of deleted metas
					index, deleted = mm.updateAndReconstruct(newBlocks, blocks, task)
				} else {
					// Faster, but leaves deleted metas
					index, deleted = mm.update(newBlocks, blocks, task)
				}

			} else {
				newBlocks[0] = make([]*m.Meta, 0, mm.blockMaxSize)
			}

			// Add new metas and if it is necessary allocate blocks for empty cell

			lastBlockIdx, _ = mm.calcIndices(index - 1)

			if len(task.ForAdd) > 0 {
				for _, meta := range task.ForAdd {
					i, _ := mm.calcIndices(index)

					// Allocate block if it is new
					if i > lastBlockIdx {
						newBlocks[i] = make([]*m.Meta, 0, mm.blockMaxSize)
						lastBlockIdx = i
					}

					newBlocks[i] = append(newBlocks[i], meta.Copy())

					// Put new meta in the sorted position
					for k := index - 1; k >= 0; k-- {
						i1, j1 := mm.calcIndices(k)
						i2, j2 := mm.calcIndices(k + 1)

						if newBlocks[i1][j1].ID < newBlocks[i2][j2].ID {
							break
						}
						newBlocks[i1][j1], newBlocks[i2][j2] = newBlocks[i2][j2], newBlocks[i1][j1]
					}
					index++
				}
			}

			// Set state

			newBlocks = newBlocks[:lastBlockIdx+1]
			version = mm.state.Set(task.Storage, newBlocks)

			if version == 0 {
				task.Trace.DEBUG(fields, "Storage '", task.Storage, "' not exists")
				return
			}

			if task.Callback != nil {
				task.Callback()
			}

			// Add deleted metas

			if len(deleted) > 0 {
				mm.usesMx.Lock()

				for _, meta := range deleted {
					name := path.Join(m.DIR_STORAGES, task.Storage, meta.Name())
					mm.deleteList[name] = version
				}

				mm.usesMx.Unlock()
				fields["deleted"] = fmt.Sprint(len(deleted))
			}

			task.Trace.DEBUG(fields, "Metas updated")
		})
	}
}

// Metas

func (mm *MetasMap) Find(trace *sl.Trace, storage string, chunkID uint64) *m.Meta {
	defer trace.AddModule("_MetasMap", "Find")()
	blocks, version := mm.state.Get(storage)

	if version == 0 {
		trace.DEBUG(nil, "Storage not exists: ", storage)
		return nil
	}

	i := mm.binSearchBlock(blocks, chunkID)
	j := -1

	if i != -1 {
		j = mm.binSearchMeta(blocks[i], chunkID)
	}

	if i == -1 || j == -1 || blocks[i][j].IsDeleted {
		trace.DEBUG(nil, "Not found chunk with ID ", chunkID, " in storage: ", storage)
		return nil
	}

	trace.DEBUG(nil, "Chunk with ID ", chunkID, " found in storage: ", storage)
	return blocks[i][j].Copy()
}

func (mm *MetasMap) GetLastVersionMetas(trace *sl.Trace, storage string, oldMetas []*m.Meta) []*m.Meta {
	defer trace.AddModule("_MetasMap", "GetLastVersionMetas")()
	blocks, version := mm.state.Get(storage)

	if version == 0 {
		trace.DEBUG(nil, "Storage not exists: ", storage)
		return nil
	}

	lastMetas := make([]*m.Meta, 0, len(oldMetas))

	for _, meta := range oldMetas {
		i := mm.binSearchBlock(blocks, meta.ID)

		if i == -1 {
			continue
		}
		j := mm.binSearchMeta(blocks[i], meta.ID)

		if j != -1 && !blocks[i][j].IsDeleted {
			lastMetas = append(lastMetas, blocks[i][j].Copy())
		}
	}

	trace.DEBUG(nil, len(lastMetas), "/", len(oldMetas), " chunks getted from storage: ", storage)
	return lastMetas
}

func (mm *MetasMap) GetInRange(trace *sl.Trace, storage string, tr m.TimeRange) []*m.Meta {
	defer trace.AddModule("_MetasMap", "GetInRange")()
	blocks, version := mm.state.Get(storage)

	if version == 0 {
		trace.DEBUG(nil, "Storage not exists: ", storage)
		return nil
	}

	inRange := []*m.Meta{}

	for i := range blocks {
		for j := range blocks[i] {
			if !blocks[i][j].IsDeleted && time_range.IsCrossed(blocks[i][j].TimeRange, tr) {
				inRange = append(inRange, blocks[i][j].Copy())
			}
		}
	}

	trace.DEBUG(nil, len(inRange), " chunks in range in storage: ", storage)
	return inRange
}

func (mm *MetasMap) GetExpired(trace *sl.Trace, storage string, deadline int64) []*m.Meta {
	defer trace.AddModule("_MetasMap", "GetExpired")()
	blocks, version := mm.state.Get(storage)

	if version == 0 {
		trace.DEBUG(nil, "Storage not exists: ", storage)
		return nil
	}
	expired := []*m.Meta{}

	for i := range blocks {
		for j := range blocks[i] {
			if blocks[i][j].TimeRange.End < deadline &&
				!blocks[i][j].IsDeleted &&
				blocks[i][j].Offsets == nil {
				expired = append(expired, blocks[i][j].Copy())
			}
		}
	}

	trace.DEBUG(nil, len(expired), " chunks expired in storage: ", storage)
	return expired
}

func (mm *MetasMap) GetFulledCrossedMetas(trace *sl.Trace, storage string) []*m.Meta {
	defer trace.AddModule("_MetasMap", "GetFulledCrossedMetas")()
	blocks, version := mm.state.Get(storage)

	if version == 0 {
		trace.DEBUG(nil, "Storage not exists: ", storage)
		return nil
	}

	metas := make([]*m.Meta, mm.metasLen(blocks))
	k := 0

	for i := range blocks {
		for j := range blocks[i] {
			if !blocks[i][j].IsDeleted {
				metas[k] = blocks[i][j]
				k++
			}
		}
	}
	metas = metas[:k]

	sort.Slice(metas, func(i, j int) bool {
		return metas[i].TimeRange.Start < metas[j].TimeRange.Start
	})

	crossedMetas := make([]*m.Meta, 0, 5)

	for i := 1; i < len(metas); i++ {
		// one of chunks is raw
		if metas[i-1].Offsets != nil || metas[i].Offsets != nil {
			if len(crossedMetas) > 0 {
				break
			}
			continue
		}

		if metas[i-1].TimeRange.End > metas[i].TimeRange.Start {
			if len(crossedMetas) == 0 {
				crossedMetas = append(crossedMetas, metas[i-1].Copy())
			}
			crossedMetas = append(crossedMetas, metas[i].Copy())

			if len(crossedMetas) == cap(crossedMetas) {
				break
			}

		} else if len(crossedMetas) > 0 {
			break
		}
	}

	trace.DEBUG(nil, len(crossedMetas), " chunks crossed in storage: ", storage)
	return crossedMetas
}
