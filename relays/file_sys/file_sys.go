package file_sys

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/google/uuid"
	sl "github.com/j-hitgate/sherlog"
	"github.com/vmihailenco/msgpack/v5"

	m "main/models"
)

type FileSys struct{}

func (*FileSys) toBytes(v any) ([]byte, error) {
	switch v := v.(type) {
	case []byte:
		return v, nil
	case [][]byte:
		return bytes.Join(v, []byte{}), nil
	case uint64:
		buff := make([]byte, 8)
		binary.LittleEndian.PutUint64(buff, v)
		return buff, nil
	default:
		return msgpack.Marshal(v)
	}
}

// Write

func (fsr *FileSys) WriteFile(trace *sl.Trace, name string, atomic bool, v any) (writedBytes int) {
	defer trace.AddModule("_FileSys", "WriteFile")()
	fields := sl.Fields{"name": name}

	data, err := fsr.toBytes(v)

	if err != nil {
		trace.FATAL(fields, "Convert to bytes error: ", err.Error())
	}

	dir := filepath.Dir(name)
	err = os.MkdirAll(dir, 0755)

	if err != nil {
		trace.FATAL(fields, "Make dir error: ", err.Error())
	}

	if !atomic {
		err = os.WriteFile(name, data, 0644)

		if err != nil {
			trace.FATAL(fields, "Writing to file err: ", err.Error())
		}

	} else {
		newName := name + ".new"
		err = os.WriteFile(newName, data, 0644)

		if err != nil {
			os.Remove(newName)
			fields["name"] = newName
			trace.FATAL(fields, "Writing to file error: ", err.Error())
		}

		err = os.Rename(newName, name)

		if err != nil {
			os.Remove(newName)
			trace.FATAL(fields, "Renameing error: ", err.Error())
		}
	}

	fields["bytes"] = fmt.Sprint(len(data))
	trace.DEBUG(fields, "File written")

	return len(data)
}

func (fsr *FileSys) WriteFileAt(trace *sl.Trace, name string, offset int64, v any) (writedBytes int) {
	defer trace.AddModule("_FileSys", "WriteFileAt")()
	fields := sl.WithFields("name", name, "offset", offset)

	data, err := fsr.toBytes(v)

	if err != nil {
		trace.FATAL(fields, "Convert to bytes error: ", err.Error())
	}

	dir := filepath.Dir(name)
	err = os.MkdirAll(dir, 0755)

	if err != nil {
		trace.FATAL(fields, "Make dir error: ", err.Error())
	}

	file, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		trace.FATAL(fields, "Open file error: ", err.Error())
	}
	defer file.Close()

	_, err = file.Seek(offset, io.SeekStart)

	if err != nil {
		trace.FATAL(fields, "Set offset on file error: ", err.Error())
	}

	writedBytes, err = file.Write(data)

	if err != nil {
		trace.FATAL(fields, "Write to file error: ", err.Error())
	}

	fields["bytes"] = fmt.Sprint(writedBytes)
	trace.DEBUG(fields, "File written")

	return writedBytes
}

func (fsr *FileSys) AppendFile(trace *sl.Trace, name string, v any) (writedBytes int) {
	defer trace.AddModule("_FileSys", "AppendFile")()
	fields := sl.Fields{"name": name}

	dir := filepath.Dir(name)
	err := os.MkdirAll(dir, 0755)

	if err != nil {
		trace.FATAL(fields, "Make dir error: ", err.Error())
	}

	file, err := os.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)

	if err != nil {
		trace.FATAL(fields, "Open file error: ", err.Error())
	}
	defer file.Close()

	data, err := fsr.toBytes(v)

	if err != nil {
		trace.FATAL(fields, "Convert to bytes error: ", err.Error())
	}

	writedBytes, err = file.Write(data)

	if err != nil {
		trace.FATAL(fields, "Write to file error: ", err.Error())
	}

	fields["bytes"] = fmt.Sprint(len(data))
	trace.DEBUG(fields, "Appended to file")

	return writedBytes
}

// Read

func (fsr *FileSys) ReadFile(trace *sl.Trace, name string) []byte {
	defer trace.AddModule("_FileSys", "ReadFile")()
	fields := sl.Fields{"name": name}

	dir := filepath.Dir(name)
	err := os.MkdirAll(dir, 0755)

	if err != nil {
		trace.FATAL(fields, "Make dir error: ", err.Error())
	}

	file, err := os.OpenFile(name, os.O_CREATE|os.O_RDONLY, 0644)

	if err != nil {
		trace.FATAL(fields, "Open file error: ", err.Error())
	}
	defer file.Close()

	info, err := file.Stat()

	if err != nil {
		trace.FATAL(fields, "Get stats from file error: ", err.Error())
	}

	buff := make([]byte, info.Size())
	_, err = file.Read(buff)

	if err != nil {
		trace.FATAL(fields, "Read file error: ", err.Error())
	}

	fields["bytes"] = fmt.Sprint(len(buff))
	trace.DEBUG(fields, "File readed")

	return buff
}

func (fsr *FileSys) ReadFileTo(trace *sl.Trace, name string, v any) (readedBytes int) {
	defer trace.AddModule("_FileSys", "ReadFileTo")()

	data := fsr.ReadFile(trace, name)

	if len(data) == 0 {
		trace.FATAL(sl.Fields{"name": name}, "File empty")
	}

	err := msgpack.Unmarshal(data, v)

	if err != nil {
		trace.FATAL(sl.Fields{"name": name}, "Convert from bytes error: ", err.Error())
	}
	return len(data)
}

func (fsr *FileSys) Exists(trace *sl.Trace, name string) bool {
	defer trace.AddModule("_FileSys", "Exists")()

	_, err := os.Stat(name)

	if err == nil {
		return true
	}

	if os.IsNotExist(err) {
		return false
	}

	trace.FATAL(sl.Fields{"name": name}, "Get stats from file error", err.Error())
	return false
}

// Remove

func (fsr *FileSys) AtomicRemove(trace *sl.Trace, names ...string) {
	defer trace.AddModule("_FileSys", "AtomicRemove")()

	tx := NewTransaction(trace, "rm_"+uuid.New().String())

	for i := range names {
		tx.Add(RemoveAction(names[i]))
	}
	tx.Commit()
	tx.Apply()
}

func (fsr *FileSys) Remove(trace *sl.Trace, name string) bool {
	defer trace.AddModule("_FileSys", "Remove")()

	err := os.Remove(name)

	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}

	trace.FATAL(sl.Fields{"name": name}, "Remove file error: ", err.Error())
	return false
}

// Dirs

func (fsr *FileSys) MakeDirAll(trace *sl.Trace, dirPath ...string) {
	defer trace.AddModule("_FileSys", "MakeDirAll")()

	dir := path.Join(dirPath...)
	err := os.MkdirAll(dir, 0755)
	fields := sl.Fields{"dir": dir}

	if err != nil {
		trace.FATAL(fields, "Make dir error: ", err.Error())
	}

	trace.DEBUG(fields, "Dir maked")
}

func (fsr *FileSys) ReadAndSendDeleteQueries(trace *sl.Trace, queue chan<- *m.DeleteQuery) {
	defer trace.AddModule("_FileSys", "ReadAndSendDeleteQueries")()

	dir := m.DIR_DELETE_TASKS
	err := os.MkdirAll(dir, 0755)

	if err != nil {
		trace.FATAL(nil, "Creating dir '", dir, "' error: ", err.Error())
	}

	entries, err := os.ReadDir(dir)

	if err != nil {
		trace.FATAL(nil, "Reading dir '", dir, "' error: ", err.Error())
	}

	for i := range entries {
		taskID := entries[i].Name()
		name := path.Join(dir, taskID)

		if strings.HasSuffix(name, ".new") {
			err = os.Remove(name)

			if err != nil {
				trace.FATAL(nil, "Remove file '", dir, "/", name, "' error: ", err.Error())
			}

		} else {
			query := &m.DeleteQuery{
				TaskID: taskID,
				ErrCh:  make(chan error, 1),
				Trace:  trace,
			}
			fsr.ReadFileTo(trace, name, query)
			queue <- query
		}
	}

	trace.DEBUG(nil, "Delete queries readed and sent")
}

func (fsr *FileSys) filterAndReadMetas(trace *sl.Trace, storage string) ([]*m.Meta, bool) {
	defer trace.AddModule("_FileSys", "filterAndReadMetas")()

	storagePath := path.Join(m.DIR_STORAGES, storage)
	entries, err := os.ReadDir(storagePath)

	if err != nil {
		trace.FATAL(nil, "Make dir '", m.DIR_STORAGES, "/'", storage, "' error: ", err.Error())
	}

	metas := make([]*m.Meta, 0, len(entries))

	// Find chunks from storage

	for i := range entries {
		if !entries[i].IsDir() {
			if entries[i].Name() == "_deleted_" {
				fsr.AtomicRemove(trace, storagePath)
				trace.DEBUG(nil, "Storage '", storage, "' removed")
				return nil, false
			}
			continue
		}

		name := entries[i].Name()
		meta, err := m.NewMetaEmpty(name)

		if err == nil {
			metas = append(metas, meta)
		}
	}

	if len(metas) == 0 {
		return []*m.Meta{}, true
	}

	// Sort metas by ID and Version

	sort.Slice(metas, func(i, j int) bool {
		if metas[i].ID == metas[j].ID {
			return metas[i].Version > metas[j].Version
		}
		return metas[i].ID < metas[j].ID
	})

	// Read and filter metas

	forRemove := []string{}
	var currID uint64
	j := 0

	for _, meta := range metas {
		chunkPath := path.Join(storagePath, meta.Name())

		// Same IDs but an older version, so remove it
		if currID == meta.ID {
			forRemove = append(forRemove, chunkPath)
			continue
		}

		metaPath := path.Join(chunkPath, "meta")
		data := fsr.ReadFile(trace, metaPath)

		// Delete chunk if it is corrupted (meta-file not found/empty)
		if len(data) == 0 {
			forRemove = append(forRemove, chunkPath)
			continue
		}

		err := msgpack.Unmarshal(data, meta)

		if err != nil {
			trace.FATAL(sl.Fields{"name": metaPath}, "Convert from bytes error: ", err.Error())
		}

		if meta.IsDeleted {
			forRemove = append(forRemove, chunkPath)
			continue
		}

		metas[j] = meta
		currID = meta.ID
		j++
	}
	metas = metas[:j]

	// remove chunks

	if len(forRemove) > 0 {
		fsr.AtomicRemove(trace, forRemove...)
	}

	trace.DEBUG(nil, "Readed ", len(metas), " chunks from storage ", storage)
	return metas[:j], true
}

func (fsr *FileSys) ReadAndClearStorages(trace *sl.Trace) (metasMap map[string][]*m.Meta, firstRawChunks map[string]uint64) {
	defer trace.AddModule("_FileSys", "ReadAndClearStorages")()

	err := os.MkdirAll(m.DIR_STORAGES, 0755)

	if err != nil {
		trace.FATAL(nil, "Make dir '", m.DIR_STORAGES, "' error: ", err.Error())
	}

	entries, err := os.ReadDir(m.DIR_STORAGES)

	if err != nil {
		trace.FATAL(nil, "Read dir '", m.DIR_STORAGES, "' error: ", err.Error())
	}

	metasMap = map[string][]*m.Meta{}
	firstRawChunks = map[string]uint64{}

	for i := range entries {
		if !entries[i].IsDir() {
			continue
		}
		storage := entries[i].Name()
		metas, ok := fsr.filterAndReadMetas(trace, storage)

		if !ok {
			continue
		}
		metasMap[storage] = metas

		if len(metas) == 0 {
			firstRawChunks[storage] = 1
			continue
		}

		// Find raw chunk
		for _, meta := range metas {
			if meta.Offsets != nil {
				firstRawChunks[storage] = meta.ID
				break
			}
		}
		if _, ok := firstRawChunks[storage]; !ok {
			firstRawChunks[storage] = metas[len(metas)-1].ID + 1
		}
	}

	trace.DEBUG(nil, "Storages readed and cleared")
	return metasMap, firstRawChunks
}
