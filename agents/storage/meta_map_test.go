package storage

import (
	"fmt"
	"path"
	"strings"
	"sync"
	"testing"

	sl "github.com/j-hitgate/sherlog"
	"github.com/stretchr/testify/assert"

	m "main/models"
	tt "main/test_tools"
)

func TestGetExpired(t *testing.T) {
	tt.SherlogInit()
	trace := sl.NewTrace("Main")

	testCases := []struct {
		name        string
		metas       []*m.Meta
		deadline    int64
		expectedIDs map[uint64]bool
	}{
		{
			name: "simple",
			metas: []*m.Meta{
				{ID: 1, TimeRange: m.TimeRange{Start: 1, End: 2}},
				{ID: 2, TimeRange: m.TimeRange{Start: 3, End: 4}},
				{ID: 3, TimeRange: m.TimeRange{Start: 5, End: 6}},
				{ID: 4, TimeRange: m.TimeRange{Start: 7, End: 8}},
				{ID: 5, TimeRange: m.TimeRange{Start: 9, End: 10}},
			},
			deadline: 6,
			expectedIDs: map[uint64]bool{
				1: true,
				2: true,
			},
		},
		{
			name: "with raw chunk",
			metas: []*m.Meta{
				{ID: 1, TimeRange: m.TimeRange{Start: 1, End: 2}},
				{ID: 2, TimeRange: m.TimeRange{Start: 3, End: 4}, Offsets: &m.Offsets{}},
				{ID: 3, TimeRange: m.TimeRange{Start: 5, End: 6}},
			},
			deadline: 8,
			expectedIDs: map[uint64]bool{
				1: true,
				3: true,
			},
		},
	}

	mm := NewMetasMap(2)

	for _, tc := range testCases {
		mm.AddStorage(trace, tc.name, tc.metas)
		expired := mm.GetExpired(trace, tc.name, tc.deadline)

		for _, meta := range expired {
			if !tc.expectedIDs[meta.ID] {
				assert.Fail(t, fmt.Sprint("Name: ", tc.name, ", Len: ", len(expired), ", ID: ", meta.ID))
				return
			}
		}
	}
}

func TestGetFulledCrossedMetas(t *testing.T) {
	tt.SherlogInit()
	trace := sl.NewTrace("Main")

	testCases := []struct {
		name        string
		metas       []*m.Meta
		expectedIDs map[uint64]bool
	}{
		{
			name: "simple",
			metas: []*m.Meta{
				{ID: 1, TimeRange: m.TimeRange{Start: 4, End: 7}},
				{ID: 2, TimeRange: m.TimeRange{Start: 1, End: 2}},
				{ID: 3, TimeRange: m.TimeRange{Start: 6, End: 8}},
				{ID: 4, TimeRange: m.TimeRange{Start: 3, End: 5}},
				{ID: 5, TimeRange: m.TimeRange{Start: 10, End: 12}},
				{ID: 6, TimeRange: m.TimeRange{Start: 9, End: 11}},
			},
			expectedIDs: map[uint64]bool{
				1: true,
				3: true,
				4: true,
			},
		},
		{
			name: "with limit",
			metas: []*m.Meta{
				{ID: 1, TimeRange: m.TimeRange{Start: 1, End: 3}},
				{ID: 2, TimeRange: m.TimeRange{Start: 2, End: 5}},
				{ID: 3, TimeRange: m.TimeRange{Start: 4, End: 6}},
				{ID: 4, TimeRange: m.TimeRange{Start: 5, End: 7}},
				{ID: 5, TimeRange: m.TimeRange{Start: 6, End: 8}},
				{ID: 6, TimeRange: m.TimeRange{Start: 7, End: 9}},
				{ID: 7, TimeRange: m.TimeRange{Start: 8, End: 10}},
			},
			expectedIDs: map[uint64]bool{
				1: true,
				2: true,
				3: true,
				4: true,
				5: true,
			},
		},
	}

	mm := NewMetasMap(3)

	for _, tc := range testCases {
		mm.AddStorage(trace, tc.name, tc.metas)
		crossed := mm.GetFulledCrossedMetas(trace, tc.name)

		for _, meta := range crossed {
			if !tc.expectedIDs[meta.ID] {
				assert.Fail(t, fmt.Sprint("Name: ", tc.name, ", Len: ", len(crossed), ", ID: ", meta.ID))
				return
			}
		}
	}
}

func TestFind(t *testing.T) {
	tt.SherlogInit()
	trace := sl.NewTrace("Main")

	metas := []*m.Meta{
		{ID: 3, IsDeleted: false},
		{ID: 4, IsDeleted: false},
		{ID: 6, IsDeleted: false},
		{ID: 7, IsDeleted: true},
		{ID: 8, IsDeleted: false},
		{ID: 9, IsDeleted: false},
	}

	testCases := []struct {
		name     string
		id       uint64
		expected uint64
	}{
		{name: "simple", id: 4, expected: metas[1].ID},
		{name: "first", id: 3, expected: metas[0].ID},
		{name: "last", id: 9, expected: metas[5].ID},
		{name: "first out", id: 1, expected: 0},
		{name: "last out", id: 10, expected: 0},
		{name: "not exists", id: 5, expected: 0},
		{name: "deleted", id: 7, expected: 0},
	}

	mm := NewMetasMap(3)
	mm.AddStorage(trace, "storage", metas)

	for _, tc := range testCases {
		meta := mm.Find(trace, "storage", tc.id)

		if tc.expected > 0 {
			assert.Equal(t, tc.expected, meta.ID, tc.name)
		} else {
			assert.Nil(t, meta, tc.name)
		}
	}
}

func TestRemoving(t *testing.T) {
	tt.SherlogInit()
	trace := sl.NewTrace("Main")

	mm := NewMetasMap(2) // version 1
	wg := &sync.WaitGroup{}

	mm.AddStorage(trace, "storage", []*m.Meta{ // version 2
		{ID: 1, Version: 1},
		{ID: 2, Version: 1},
		{ID: 3, Version: 1},
	})

	wg.Add(1)
	mm.Update(&m.UpdateStateTask{ // version 3
		Storage:   "storage",
		ForUpdate: []*m.Meta{{ID: 1, Version: 2, IsDeleted: true}},
		Trace:     trace,
		Callback:  func() { wg.Done() },
	})
	wg.Wait()

	unreserve := mm.ReserveVersion(trace, &struct{}{}) // reserve versions begin 3

	wg.Add(1)
	mm.Update(&m.UpdateStateTask{ // version 4
		Storage:   "storage",
		ForUpdate: []*m.Meta{{ID: 2, Version: 2, IsDeleted: true}},
		Trace:     trace,
		Callback:  func() { wg.Done() },
	})
	wg.Wait()

	forRemove := mm.GetForRemove()

	if len(forRemove) != 1 || forRemove[0] != path.Join("storages", "storage", "1_1") {
		assert.Fail(t, fmt.Sprint("1. Len: ", len(forRemove), ", Names: ", strings.Join(forRemove, ", ")))
		return
	}

	unreserve(trace) // unreserve versions begin 3

	forRemove = mm.GetForRemove()

	if len(forRemove) != 1 || forRemove[0] != path.Join("storages", "storage", "2_1") {
		assert.Fail(t, fmt.Sprint("2. Len: ", len(forRemove), ", Names: ", strings.Join(forRemove, ", ")))
	}
}
