package tickdata

import (
	"time"
)

type DayIterator func(ticks []*TickData, err error) bool
type TickIterator func(tick *TickData, err error) bool

type Day interface {
	Symbol() string
	Time() time.Time
	EachDay(it DayIterator)
	EachTick(it TickIterator)
}
