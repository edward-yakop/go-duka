package stream

import (
	"ed-fx/go-duka/api/tickdata"
	iTickdata "ed-fx/go-duka/internal/tickdata"
	"time"
	"unknwon.dev/clog/v2"
)

// time is in UTC
type Iterator func(time time.Time, tick *tickdata.TickData, err error) bool

type Stream struct {
	symbol             string
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
	for t := downloadStart(start); t.Before(dEnd) && isContinue; t = t.Add(24 * time.Hour) {
		day, err := iTickdata.FetchDay(s.symbol, t, s.downloadFolderPath)
		if err != nil && !it(t, nil, err) {
			return
		}
		day.EachTick(func(tick *tickdata.TickData, err error) bool {
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
	dStart = time.Date(dStart.Year(), dStart.Month(), dStart.Day(), 0, 0, 0, 0, time.UTC)
	return dStart
}

func downloadEnd(end time.Time) time.Time {
	dEnd := end.UTC()
	dEnd = time.Date(dEnd.Year(), dEnd.Month(), dEnd.Day(), 23, 59, 59, 0, time.UTC)
	return dEnd
}

var isLogSetup = false

// time are in UTC
func NewStream(symbol string, start time.Time, end time.Time, downloadFolderPath string) *Stream {
	if !isLogSetup {
		clog.NewConsole(0, clog.ConsoleConfig{
			Level: clog.LevelInfo,
		})
	}

	return &Stream{
		symbol:             symbol,
		start:              start,
		end:                end,
		downloadFolderPath: downloadFolderPath,
	}
}
