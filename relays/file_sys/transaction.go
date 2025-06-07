package file_sys

import (
	"os"
	"path"
	"strings"

	sl "github.com/j-hitgate/sherlog"
	"github.com/vmihailenco/msgpack/v5"

	m "main/models"
)

type actionType byte

const (
	cut actionType = iota
	remove
	rename
)

type action struct {
	Type    actionType
	Name    string
	Size    *int64 `msgpack:",omitempty"`
	NewName string `msgpack:",omitempty"`
}

type Transaction struct {
	name    string
	actions []*action
	fileSys *FileSys
	trace   *sl.Trace
}

func NewTransaction(trace *sl.Trace, name string) *Transaction {
	return &Transaction{
		name:    path.Join(m.DIR_TRANSACTIONS, name),
		actions: []*action{},
		fileSys: &FileSys{},
		trace:   trace,
	}
}

func NewTransactionFromFile(trace *sl.Trace, name string) *Transaction {
	tx := &Transaction{
		fileSys: &FileSys{},
		trace:   trace,
	}
	tx.ReadTransaction(name)
	return tx
}

func (tx *Transaction) ReadTransaction(name string) {
	defer tx.trace.AddModule("_Transaction", "ReadTransaction")()

	name = path.Join(m.DIR_TRANSACTIONS, name)
	data := tx.fileSys.ReadFile(tx.trace, name)

	actions := []*action{}
	err := msgpack.Unmarshal(data, &actions)

	if err != nil {
		tx.trace.FATAL(sl.Fields{"name": name}, "Convert from bytes error: ", err.Error())
	}

	tx.name = name
	tx.actions = actions
}

func (tx *Transaction) Add(act *action) {
	tx.actions = append(tx.actions, act)
}

func (tx *Transaction) Commit() {
	defer tx.trace.AddModule("_Transaction", "Commit")()

	if len(tx.actions) == 0 {
		return
	}

	data, err := msgpack.Marshal(tx.actions)

	if err != nil {
		tx.trace.FATAL(sl.Fields{"transaction": tx.name}, "Convert to bytes error: ", err.Error())
	}

	tx.fileSys.WriteFile(tx.trace, tx.name, true, data)
}

func (tx *Transaction) Cancel() {
	defer tx.trace.AddModule("_Transaction", "Cancel")()

	if len(tx.actions) == 0 {
		return
	}

	err := os.Remove(tx.name)

	if err != nil {
		tx.trace.FATAL(nil, "Remove transaction '", tx.name, "' error: ", err.Error())
	}
	tx.actions = []*action{}
}

func (tx *Transaction) Apply() {
	defer tx.trace.AddModule("_Transaction", "Apply")()
	var err error

	for _, act := range tx.actions {
		switch act.Type {
		case cut:
			err = os.Truncate(act.Name, *act.Size)

			if err != nil && !os.IsNotExist(err) {
				tx.trace.FATAL(
					sl.WithFields("name", act.Name, "size", act.Size),
					"Truncate file error: ", err.Error(),
				)
			}

		case remove:
			err = os.RemoveAll(act.Name)

			if err != nil && !os.IsNotExist(err) {
				tx.trace.FATAL(sl.Fields{"name": act.Name}, "Remove file/dir error: ", err.Error())
			}

		case rename:
			err = os.Rename(act.Name, act.NewName)

			if err != nil && !os.IsNotExist(err) {
				tx.trace.FATAL(sl.Fields{"name": act.Name}, "Rename file error: ", err.Error())
			}

		default:
			tx.trace.FATAL(sl.Fields{"name": act.Name}, "Incorrect action:", act.Type)
		}
	}

	err = os.Remove(tx.name)

	if err != nil {
		tx.trace.FATAL(nil, "Remove transaction '", tx.name, "' error: ", err.Error())
	}
	tx.actions = []*action{}
}

// Actions

func CutAction(name string, size int64) *action {
	return &action{Type: cut, Name: name, Size: &size}
}

func RemoveAction(name string) *action {
	return &action{Type: remove, Name: name}
}

func RenameAction(name string, newName string) *action {
	return &action{Type: rename, Name: name, NewName: newName}
}

// Functions

func RunTransactions(trace *sl.Trace) {
	defer trace.AddModule("", "RunTransactions")()

	err := os.MkdirAll(m.DIR_TRANSACTIONS, 0755)

	if err != nil {
		trace.FATAL(nil, "Make dir '", m.DIR_TRANSACTIONS, "' error: ", err.Error())
	}

	entries, err := os.ReadDir(m.DIR_TRANSACTIONS)

	if err != nil {
		trace.FATAL(nil, "Read dir '", m.DIR_TRANSACTIONS, "' error: ", err.Error())
	}

	for i := range entries {
		name := entries[i].Name()

		if strings.HasSuffix(name, ".new") {
			err = os.Remove(name)

			if err != nil {
				trace.FATAL(nil, "Remove file '", m.DIR_TRANSACTIONS, "/", name, "' error: ", err.Error())
			}

		} else {
			tx := NewTransactionFromFile(trace, name)
			tx.Apply()
		}
	}
	trace.DEBUG(nil, len(entries), " transactions runned")
}
