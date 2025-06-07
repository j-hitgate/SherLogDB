package file_sys

import (
	"fmt"
	"os"
	"path"
	"testing"

	sl "github.com/j-hitgate/sherlog"
	"github.com/stretchr/testify/assert"

	tt "main/test_tools"
)

func TestTransaction(t *testing.T) {
	tt.SherlogInit()
	trace := sl.NewTrace("Main")

	// Prepare files

	os.WriteFile("forCut.txt", []byte("1234567890"), 0644)
	os.WriteFile("forRemove.txt", []byte("abc"), 0644)
	os.WriteFile("forRename.txt", []byte("abc"), 0644)

	defer func() {
		os.RemoveAll("transactions")
		os.Remove("forCut.txt")
		os.Remove("renamed.txt")
	}()

	// Make transaction

	tx := NewTransaction(trace, "tx")

	tx.Add(CutAction("forCut.txt", 5))
	tx.Add(RemoveAction("forRemove.txt"))
	tx.Add(RenameAction("forRename.txt", "renamed.txt"))
	tx.Commit()

	_, err := os.Stat(path.Join("transactions", "tx"))

	if err != nil {
		assert.Fail(t, "Transaction not be created: ", err.Error())
		return
	}

	tx.ReadTransaction("tx") // Check readability of transaction
	tx.Apply()

	_, err = os.Stat(path.Join("transactions", "tx"))

	if !os.IsNotExist(err) {
		assert.Fail(t, "Transaction not be removed after applaing")
		return
	}

	// Check files

	info, _ := os.Stat("forCut.txt")

	if info.Size() != 5 {
		assert.Fail(t, fmt.Sprint("Size 'forCut.txt' must be 5, actual ", info.Size()))
		return
	}

	_, err = os.Stat("forRemove.txt")

	if !os.IsNotExist(err) {
		assert.Fail(t, "File 'forRemove.txt' not be deleted")
		return
	}

	_, err = os.Stat("renamed.txt")

	if err != nil {
		assert.Fail(t, "File 'forRename.txt' not be renamed to 'renamed.txt': ", err.Error())
	}
}
