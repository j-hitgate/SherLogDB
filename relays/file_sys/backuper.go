package file_sys

import (
	"os"
	"path"
	"strings"

	sl "github.com/j-hitgate/sherlog"

	m "main/models"
)

type Backuper struct {
	name     string
	backupTx *Transaction
	cancelTx *Transaction
	fileSys  *FileSys
	trace    *sl.Trace
}

func NewBackuper(trace *sl.Trace, name string) *Backuper {
	return &Backuper{
		name:     name,
		backupTx: NewTransaction(trace, name),
		fileSys:  &FileSys{},
		trace:    trace,
	}
}

func (b *Backuper) AddForCut(name string) (size int64) {
	defer b.trace.AddModule("_Backuper", "AddForCut")()

	info, err := os.Stat(name)

	if err == nil {
		size = info.Size()
		b.backupTx.Add(CutAction(name, size))

	} else if os.IsNotExist(err) {
		b.backupTx.Add(RemoveAction(name))

	} else {
		b.trace.FATAL(sl.Fields{"name": name}, "Get stats from file error: ", err.Error())
	}

	return size
}

func (b *Backuper) AddForCutToSize(name string, size int64) {
	popModule := b.trace.AddModule("_Backuper", "AddForCutToSize")
	b.backupTx.Add(CutAction(name, size))
	popModule()
}

func (b *Backuper) AddForReplace(name string) {
	defer b.trace.AddModule("_Backuper", "AddForReplace")()

	if !strings.HasSuffix(name, ".new") {
		b.trace.FATAL(nil, "No has extends '.new': ", name)
	}

	b.backupTx.Add(RemoveAction(name))

	if b.cancelTx == nil {
		b.cancelTx = NewTransaction(b.trace, b.name)
	}
	b.cancelTx.Add(RenameAction(name, name[:len(name)-4]))
}

func (b *Backuper) AddChunk(chunkPath string, offsets *m.Offsets) {
	defer b.trace.AddModule("_Transaction", "AddChunk")()
	var name string

	if offsets == nil || offsets.IsZero() {
		b.backupTx.Add(RemoveAction(chunkPath))

	} else {
		columns := m.GetLogColumns()

		for i := range columns {
			offset, _ := offsets.Get(columns[i])
			name = path.Join(chunkPath, string(columns[i]))
			b.backupTx.Add(CutAction(name, *offset))
		}
	}
	name = path.Join(chunkPath, "meta.new")
	b.AddForReplace(name)
}

func (b *Backuper) Commit() {
	b.backupTx.Commit()
}

func (b *Backuper) Cancel() {
	if b.cancelTx != nil {
		b.cancelTx.Commit()
		b.cancelTx.Apply()
	} else {
		b.backupTx.Cancel()
	}
}

func (b *Backuper) Backup() {
	b.backupTx.Apply()
}

func (b *Backuper) BackupIfErr(err error) {
	if err != nil {
		b.Backup()
	} else {
		b.Cancel()
	}
}
