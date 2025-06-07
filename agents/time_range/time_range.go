package time_range

import (
	m "main/models"
)

func IsCrossed(tr1, tr2 m.TimeRange) bool {
	return !((tr1.End != 0 && tr2.Start != 0 && tr1.End < tr2.Start) ||
		(tr2.End != 0 && tr1.Start != 0 && tr2.End < tr1.Start))
}

func IsInside(outer, inner m.TimeRange) bool {
	return !((outer.Start != 0 && inner.Start != 0 && inner.Start < outer.Start) ||
		(outer.End != 0 && inner.End != 0 && outer.End < inner.End))
}
