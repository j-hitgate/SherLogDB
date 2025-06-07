package tools

import (
	"fmt"
	"sort"
	"strings"
)

type Ordered interface {
	int | int64 | uint64 | string
}

type IComparable[T any] interface {
	Equals(T) bool
}

// Slice

func CheckSliceItemsType[T any](arr []any) bool {
	for _, val := range arr {
		switch val.(type) {
		case T:
		default:
			return false
		}
	}
	return true
}

func JoinSlices[T any](arrs ...[]T) []T {
	length := 0

	for i := range arrs {
		length += len(arrs[i])
	}

	arr := make([]T, length)
	k := 0

	for i := range arrs {
		for j := range arrs[i] {
			arr[k] = arrs[i][j]
			k++
		}
	}

	return arr
}

func JoinSlicesRevers[T any](arrs ...[]T) []T {
	length := 0

	for i := range arrs {
		length += len(arrs[i])
	}

	arr := make([]T, length)
	k := len(arr) - 1

	for i := len(arrs) - 1; i >= 0; i-- {
		for j := len(arrs[i]) - 1; j >= 0; j-- {
			arr[k] = arrs[i][j]
			k--
		}
	}

	return arr
}

type SortBlock[Tval any, Tkey Ordered] struct {
	SortKey Tkey
	Value   Tval
}

func SortWithKey[Tval any, Tkey Ordered](values []Tval, valToKey func(Tval) Tkey, desc bool) {
	if len(values) < 2 {
		return
	}

	blocks := make([]*SortBlock[Tval, Tkey], len(values))

	for i := range values {
		blocks[i] = &SortBlock[Tval, Tkey]{
			SortKey: valToKey(values[i]),
			Value:   values[i],
		}
	}

	if desc {
		sort.Slice(blocks, func(i, j int) bool {
			return blocks[i].SortKey < blocks[j].SortKey
		})
	} else {
		sort.Slice(blocks, func(i, j int) bool {
			return blocks[i].SortKey > blocks[j].SortKey
		})
	}

	for i := range values {
		values[i] = blocks[i].Value
	}
}

func RemoveRange[T any](arr []T, start, end int) []T {
	if end < start {
		panic("end index before start index")
	}

	delta := end - start + 1
	newLen := len(arr) - delta

	for i := start; i < newLen; i++ {
		arr[i] = arr[i+delta]
	}

	return arr[:newLen]
}

func Contains[T comparable](value T, arr []T) bool {
	for _, val := range arr {
		if val == value {
			return true
		}
	}
	return false
}

func CastSlice[T any](arr []any) []T {
	newArr := make([]T, len(arr))

	for i, val := range arr {
		newArr[i] = val.(T)
	}

	return newArr
}

func EqualSlices[T comparable](arr1, arr2 []T) bool {
	if len(arr1) != len(arr2) {
		return false
	}

	for i := 0; i < len(arr1); i++ {
		if arr1[i] != arr2[i] {
			return false
		}
	}
	return true
}

func DeepEqualSlices[T2 any, T1 IComparable[T2]](arr1 []T1, arr2 []T2) bool {
	if len(arr1) != len(arr2) {
		return false
	}

	for i := 0; i < len(arr1); i++ {
		if !arr1[i].Equals(arr2[i]) {
			return false
		}
	}
	return true
}

func EqualSlicesBy[T any](arr1, arr2 []T, compare func(T, T) bool) bool {
	if len(arr1) != len(arr2) {
		return false
	}

	for i := 0; i < len(arr1); i++ {
		if !compare(arr1[i], arr2[i]) {
			return false
		}
	}
	return true
}

// Map

func EqualMaps[Tkey, Tval comparable](m1, m2 map[Tkey]Tval) bool {
	if len(m1) != len(m2) {
		return false
	}

	for key, val1 := range m1 {
		if val2, ok := m2[key]; !ok || val1 != val2 {
			return false
		}
	}

	return true
}

func DeepEqualMaps[Tkey comparable, Tval2 any, Tval1 IComparable[Tval2]](m1 map[Tkey]Tval1, m2 map[Tkey]Tval2) bool {
	if len(m1) != len(m2) {
		return false
	}

	for key, val1 := range m1 {
		if val2, ok := m2[key]; !ok || !val1.Equals(val2) {
			return false
		}
	}

	return true
}

func EqualMapsBy[Tkey comparable, Tval any](m1, m2 map[Tkey]Tval, compare func(Tval, Tval) bool) bool {
	if len(m1) != len(m2) {
		return false
	}

	for key, val1 := range m1 {
		if val2, ok := m2[key]; !ok || !compare(val1, val2) {
			return false
		}
	}

	return true
}

func KeysToSlice[Tkey comparable, Tval any](m map[Tkey]Tval) []Tkey {
	keys := make([]Tkey, len(m))
	i := 0

	for key := range m {
		keys[i] = key
		i++
	}
	return keys
}

// Other

func ToOrderedValue(v any) any {
	switch v := v.(type) {
	case []string:
		return strings.Join(v, ",")

	case map[string]string:
		result := make([]string, len(v))

		for key, val := range v {
			result = append(result, fmt.Sprint(key, ":", val))
		}
		return strings.Join(result, ",")
	default:
		return v
	}
}

func Min[T Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}
