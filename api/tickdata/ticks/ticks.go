package ticks

import (
	"github.com/ed-fx/go-duka/api/instrument"
	"github.com/ed-fx/go-duka/api/tickdata"
	"github.com/ed-fx/go-duka/internal/bi5"
	"github.com/pkg/errors"
	"time"
	"unknwon.dev/clog/v2"
)

type Ticks struct {
	instrument         *instrument.Metadata
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

	to = to.In(time.UTC) // To ease debugging
	t.isCompleted = false
	for currTime := t.timeToHour(to); currTime.Before(t.end); currTime = currTime.Add(time.Hour) {
		if t.ticksDayHour.Equal(currTime) {
			return t.resetTicksPointer(to)
		} else {
			bi := bi5.New(currTime, t.instrument, t.downloadFolderPath)

			// Download might return errors when there's no tick data during weekend or holiday
			if bi.Download() == nil {
				t.ticks, err = bi.Ticks()
				t.ticksIdx = 0
				t.ticksDayHour = currTime
				if err != nil {
					t.complete()
					return
				} else if len(t.ticks) != 0 {
					t.seek(to)
					return true, nil
				}
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

func (t *Ticks) seek(target time.Time) {
	count := len(t.ticks)
	i := t.ticksIdx
	for ; i < count; i++ {
		tickTime := t.ticks[i].UTC()
		if tickTime.After(target) {
			break
		}
	}

	if i > 0 {
		i--
	}
	t.ticksIdx = i
	t.currTick = t.ticks[i]
}

func (t Ticks) resetTicksPointer(to time.Time) (bool, error) {
	if t.currTick == nil { // If beginning of hour
		return t.Next()
	}

	currTickTime := t.currTick.UTC()
	if currTickTime.Before(to) {
		t.seek(to)
		return true, nil
	}

	// CurrentTick before target is handled above,
	// We only need to search backward
	prevTick := t.prevTick()
	for {
		if currTickTime.Equal(to) {
			return true, nil
		} else {
			if prevTick != nil {
				prevTrickTime := prevTick.UTC()
				if prevTrickTime.Before(to) {
					return true, nil
				} else {
					// Prev is Equal or before, either way, we need to go left
					t.ticksIdx--
					t.currTick = prevTick

					if prevTrickTime.Equal(to) {
						// If it's Equal, we're done
						return true, nil
					}
				}
			} else {
				t.ticksIdx = 0
				t.currTick = t.ticks[0]
				return true, nil
			}
		}
	}
}

func (t Ticks) prevTick() *tickdata.TickData {
	if t.ticksIdx == 0 {
		return nil
	}
	return t.ticks[t.ticksIdx-1]
}

func (t Ticks) timeToHour(tt time.Time) time.Time {
	return time.Date(tt.Year(), tt.Month(), tt.Day(), tt.Hour(), 0, 0, 0, tt.Location()).UTC()
}

var isLogSetup = false

// time are in UTC
func New(instrument *instrument.Metadata, start time.Time, end time.Time, downloadFolderPath string) *Ticks {
	if !isLogSetup {
		isLogSetup = true
		_ = clog.NewConsole(0, clog.ConsoleConfig{
			Level: clog.LevelInfo,
		})
	}

	return &Ticks{
		instrument:         instrument,
		start:              start,
		end:                end,
		downloadFolderPath: downloadFolderPath,

		ticksDayHour: time.Time{},
		ticksIdx:     -1,
		isCompleted:  false,
	}
}
