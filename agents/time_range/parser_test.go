package time_range

import (
	"testing"
	"time"

	sl "github.com/j-hitgate/sherlog"
	"github.com/stretchr/testify/assert"

	m "main/models"
	tt "main/test_tools"
)

func TestParser(t *testing.T) {
	tt.SherlogInit()
	trace := sl.NewTrace("Main")

	trp := NewParser(trace)

	// Simple range

	tr, err := trp.Parse("5 - 10")

	if err != nil {
		assert.Fail(t, "Parse simple range error: "+err.Error())
		return
	}

	expected := m.TimeRange{Start: 5, End: 10}

	if !assert.Equal(t, expected, tr, "Incorrect parsing simple range") {
		return
	}

	// Relative range

	tr, err = trp.Parse("after 5")

	if err != nil {
		assert.Fail(t, "Parse relative range error: "+err.Error())
		return
	}

	expected = m.TimeRange{Start: 5}

	if !assert.Equal(t, expected, tr, "Incorrect parsing relative range") {
		return
	}

	// Parse duration

	delta, err := trp.ParseDuration("1w 2d 5h 3m 18s")

	if err != nil {
		assert.Fail(t, "Parse duration error: "+err.Error())
		return
	}

	expectedDelta := time.Hour*24*7 + time.Hour*24*2 + time.Hour*5 + time.Minute*3 + time.Second*18

	if delta != expectedDelta {
		assert.Fail(t, "Incorrect parsing relative range")
	}
}
