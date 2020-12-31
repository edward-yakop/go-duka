package app

import (
	"ed-fx/go-duka/api/tickdata"
	"ed-fx/go-duka/internal/bi5"
	"sort"
	"time"
)

type dayHourResult struct {
	time time.Time

	bi  *bi5.Bi5
	err error
}

type TickdataDay struct {
	symbol string
	time   time.Time

	results  []*dayHourResult
	resultCh chan *dayHourResult
}

func (d TickdataDay) Symbol() string {
	return d.symbol
}

func (d TickdataDay) Time() time.Time {
	return d.time
}

func (d TickdataDay) Each(it tickdata.Iterator) {
	for _, r := range d.results {
		if r.err != nil {
			it(nil, r.err)
		} else {
			ticks, err := r.bi.Ticks()
			it(ticks, err)
		}
	}
}

func (d *TickdataDay) append(dayHour time.Time, bi *bi5.Bi5, err error) {
	d.results = append(
		d.results,
		&dayHourResult{
			time: dayHour,
			bi:   bi,
			err:  err,
		},
	)
}

func (d *TickdataDay) postConstruct() {
	close(d.resultCh)
	for bi := range d.resultCh {
		d.results = append(d.results, bi)
	}
	sort.Slice(d.results, func(i, j int) bool {
		return d.results[i].time.Before(d.results[j].time)
	})
}

func newDay(symbol string, time time.Time) *TickdataDay {
	return &TickdataDay{
		symbol:   symbol,
		time:     time,
		results:  make([]*dayHourResult, 0),
		resultCh: make(chan *dayHourResult),
	}
}
