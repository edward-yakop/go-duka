package ticks

import (
	"github.com/ed-fx/go-duka/api/tickdata"
	"github.com/ed-fx/go-duka/internal/bi5"
	"github.com/pkg/errors"
	"time"
	"unknwon.dev/clog/v2"
)

type Ticks struct {
	symbol             string
	start              time.Time
	end                time.Time
	downloadFolderPath string

	currTick     *tickdata.TickData
	ticksIdx     int
	ticks        []*tickdata.TickData
	ticksDayHour time.Time
	isCompleted  bool
}

func (t Ticks) Start() time.Time {
	return t.start
}

func (t Ticks) End() time.Time {
	return t.end
}

func (t Ticks) Current() *tickdata.TickData {
	return t.currTick
}

func (t Ticks) IsCompleted() bool {
	return t.isCompleted
}

func (t *Ticks) Next() (isSuccess bool, err error) {
	if t.isCompleted {
		return
	}

	// IF ticks loaded, let's check whether it's between boundaries
	if !t.ticksDayHour.IsZero() {
		nextTickIdx := t.ticksIdx + 1
		if nextTickIdx < len(t.ticks) {
			nextTick := t.ticks[nextTickIdx]
			nextTickTime := nextTick.UTC()
			if nextTickTime.Before(t.end) || nextTickTime.Equal(t.end) {
				t.currTick = nextTick
				t.ticksIdx = nextTickIdx

				isSuccess = true
				return
			} else {
				t.complete()
				return
			}
		}
	}

	return t.Goto(t.nextDownloadHour())
}

func (t *Ticks) Goto(to time.Time) (isSuccess bool, err error) {
	if to.Before(t.start) || to.After(t.end) {
		return false, errors.New("[" + to.String() + "] is after [" + t.end.String() + "]")
	}

	to = to.In(time.UTC) // Done to ease debugging
	t.isCompleted = false
	for currTime := to; currTime.Before(t.end); currTime = currTime.Add(time.Hour) {
		bi := bi5.New(currTime, t.symbol, t.downloadFolderPath)

		// Download might return errors when there's no tick data during weekend or holiday
		if bi.Download() == nil {
			t.ticks, err = bi.Ticks()
			if err != nil {
				t.complete()
				return
			} else if len(t.ticks) != 0 {
				t.ticksDayHour = currTime
				t.ticksIdx = t.searchTickIdx()
				t.currTick = nil

				isSuccess, err = t.Next()
				currTick := t.currTick
				for isSuccess && (to.After(currTick.UTC()) || to.Equal(currTick.UTC())) {
					isSuccess, err = t.Next()
					currTick = t.Current()
				}

				return
			}
		}
	}

	t.complete()
	return
}

func (t *Ticks) complete() {
	t.isCompleted = true
	t.ticksIdx = -1
	t.ticks = nil
	t.currTick = nil
}

func (t Ticks) nextDownloadHour() time.Time {
	var next time.Time
	if t.currTick == nil {
		next = t.start.UTC()
	} else {
		next = t.currTick.UTC().Add(time.Hour)
	}

	return time.Date(next.Year(), next.Month(), next.Day(), next.Hour(), 0, 0, 0, time.UTC)
}

func (t Ticks) searchTickIdx() (idx int) {
	count := len(t.ticks)
	for idx = 0; idx < count; idx++ {
		tick := t.ticks[idx]
		if !tick.UTC().Before(t.start) {
			break
		}
	}

	return idx - 1
}

var isLogSetup = false

// time are in UTC
func New(symbol string, start time.Time, end time.Time, downloadFolderPath string) *Ticks {
	if !isLogSetup {
		isLogSetup = true
		clog.NewConsole(0, clog.ConsoleConfig{
			Level: clog.LevelInfo,
		})
	}

	return &Ticks{
		symbol:             symbol,
		start:              start,
		end:                end,
		downloadFolderPath: downloadFolderPath,

		ticksDayHour: time.Time{},
		ticksIdx:     -1,
		isCompleted:  false,
	}
}
