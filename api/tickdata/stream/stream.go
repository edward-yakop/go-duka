package stream

import (
	"github.com/edward-yakop/go-duka/api/instrument"
	"github.com/edward-yakop/go-duka/api/tickdata"
	"github.com/edward-yakop/go-duka/internal/bi5"
	"time"
)

// time is in UTC
type Iterator func(time time.Time, tick *tickdata.TickData, err error) bool

type Stream struct {
	instrument         *instrument.Metadata
	start              time.Time
	end                time.Time
	downloadFolderPath string
}

func (s Stream) Start() time.Time {
	return s.start
}

func (s Stream) End() time.Time {
	return s.end
}

func (s Stream) EachTick(it Iterator) {
	start := s.start
	loc := start.Location()
	end := s.end.In(loc)

	dEnd := downloadEnd(s.end)
	var isContinue = true
	for t := downloadStart(start); t.Before(dEnd) && isContinue; t = t.Add(time.Hour) {
		bi := bi5.New(t, s.instrument, s.downloadFolderPath)
		err := bi.Download()
		if err != nil && !it(t.In(loc), nil, err) {
			return
		}

		bi.EachTick(func(tick *tickdata.TickData, err error) bool {
			if tick == nil {
				return true
			}
			tickTime := tick.TimeInLocation(loc)
			if (start.Equal(tickTime) || start.Before(tickTime)) &&
				(end.Equal(tickTime) || end.After(tickTime)) {
				isContinue = it(tickTime, tick, err)
			}
			return isContinue
		})
	}
}

func downloadStart(start time.Time) time.Time {
	dStart := start.UTC()
	dStart = time.Date(dStart.Year(), dStart.Month(), dStart.Day(), dStart.Hour(), 0, 0, 0, time.UTC)
	return dStart
}

func downloadEnd(end time.Time) time.Time {
	dEnd := end.UTC()
	dEnd = time.Date(dEnd.Year(), dEnd.Month(), dEnd.Day(), dEnd.Hour(), 59, 59, 0, time.UTC)
	return dEnd
}

// time are in UTC
func New(instrument *instrument.Metadata, start time.Time, end time.Time, downloadFolderPath string) *Stream {
	return &Stream{
		instrument:         instrument,
		start:              start,
		end:                end,
		downloadFolderPath: downloadFolderPath,
	}
}
