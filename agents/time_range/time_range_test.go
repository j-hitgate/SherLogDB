package time_range

import (
	"testing"

	"github.com/stretchr/testify/assert"

	m "main/models"
)

func TestTimeRange(t *testing.T) {
	// Crossing

	assert.True(t, IsCrossed(
		m.TimeRange{Start: 1, End: 5},
		m.TimeRange{Start: 3, End: 8},
	), "ranges is crossed")

	assert.False(t, IsCrossed(
		m.TimeRange{Start: 1, End: 3},
		m.TimeRange{Start: 5, End: 8},
	), "ranges is not crossed")

	// Insiding

	assert.True(t, IsInside(
		m.TimeRange{Start: 1, End: 10},
		m.TimeRange{Start: 3, End: 7},
	), "range are inside other")

	assert.False(t, IsInside(
		m.TimeRange{Start: 5, End: 10},
		m.TimeRange{Start: 2, End: 7},
	), "range are not inside other")
}
