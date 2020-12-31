package tickdata

import (
	"time"
)

type Iterator func(ticks []*TickData, err error)

type Day interface {
	Symbol() string
	Time() time.Time
	Each(it Iterator)
}
