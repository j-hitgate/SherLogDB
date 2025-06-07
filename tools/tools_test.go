package tools

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSortWithKey(t *testing.T) {
	keyMap := map[string]int64{
		"one":   1,
		"two":   2,
		"three": 3,
		"four":  4,
		"five":  5,
	}
	values := []string{"two", "five", "one", "four", "three"}

	SortWithKey(values, func(val string) int64 {
		return keyMap[val]
	}, true)

	expected := []string{"one", "two", "three", "four", "five"}

	data, _ := json.Marshal(values)
	println(string(data))

	assert.True(t, EqualSlices(expected, values), "standart")
}
