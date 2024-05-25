package tickdata

import (
	"github.com/edward-yakop/go-duka/api/instrument"
	"github.com/edward-yakop/go-duka/api/tickdata"
	"github.com/edward-yakop/go-duka/internal/bi5"
	"github.com/pkg/errors"
	"sort"
	"sync"
	"time"
)

const noParallelDownloads = 3

type dayHourResult struct {
	time time.Time

	bi  *bi5.Bi5
	err error
}

type Day struct {
	instrument *instrument.Metadata
	time       time.Time

	results  []*dayHourResult
	resultCh chan *dayHourResult
}

var _ tickdata.Day = &Day{}

func (d Day) Symbol() string {
	return d.instrument.Code()
}

func (d Day) Instrument() *instrument.Metadata {
	return d.instrument
}

func (d Day) Time() time.Time {
	return d.time
}

func (d Day) EachDay(it tickdata.DayIterator) {
	for _, r := range d.results {
		if r.err != nil {
			if !it(nil, r.err) {
				return
			}
		} else {
			ticks, err := r.bi.Ticks()
			if !it(ticks, err) {
				return
			}
		}
	}
}

func (d Day) EachTick(it tickdata.TickIterator) {
	for _, r := range d.results {
		if r.err != nil {
			if !it(nil, r.err) {
				return
			}
		} else {
			isContinue := true
			r.bi.EachTick(func(tick *tickdata.TickData, err error) bool {
				isContinue = it(tick, err)
				return isContinue
			})
			if !isContinue {
				return
			}
		}
	}
}

func (d *Day) append(dayHour time.Time, bi *bi5.Bi5, err error) {
	d.results = append(
		d.results,
		&dayHourResult{
			time: dayHour,
			bi:   bi,
			err:  err,
		},
	)
}

func (d *Day) postConstruct() {
	close(d.resultCh)
	for bi := range d.resultCh {
		d.results = append(d.results, bi)
	}
	sort.Slice(d.results, func(i, j int) bool {
		return d.results[i].time.Before(d.results[j].time)
	})
}

func FetchDay(instrument *instrument.Metadata, day time.Time, folderPath string) (result tickdata.Day, err error) {
	day = day.UTC()
	day = time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.UTC)

	// Worker s get url from this channel
	hours := make(chan int)

	go func() {
		for i := 0; i < 24; i++ {
			hours <- i
		}
		close(hours)
	}()

	var wg sync.WaitGroup
	td := newDay(instrument, day)
	for i := 0; i < noParallelDownloads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for hour := range hours {
				dayHour := day.Add(time.Duration(hour) * time.Hour)
				bi := bi5.New(dayHour, instrument, folderPath)
				derr := bi.Download()
				if derr != nil {
					derr = errors.Wrap(err, "Download Bi5 ["+dayHour.Format("2006-01-02 15")+"] failed")
				}
				td.append(dayHour, bi, derr)
			}
		}()
	}

	wg.Wait()
	td.postConstruct()

	result = td
	return
}

func newDay(instrument *instrument.Metadata, time time.Time) *Day {
	return &Day{
		instrument: instrument,
		time:       time,
		results:    make([]*dayHourResult, 0),
		resultCh:   make(chan *dayHourResult),
	}
}
