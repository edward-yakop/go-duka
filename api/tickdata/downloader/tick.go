package downloader

import (
	"github.com/ed-fx/go-duka/api/instrument"
	"github.com/ed-fx/go-duka/internal/bi5"
	"github.com/ed-fx/go-duka/internal/misc"
	"time"
)

type DownloadListener func(instrumentCode string, dayHour time.Time, err error, curr, count int)

type TickDownloader interface {
	Add(instrument *instrument.Metadata, from, to time.Time) TickDownloader
	Download(DownloadListener)
	Count() int
}

type hours struct {
	instrumentCode string
	_hours         map[time.Time]bool
}

func newHours(instrumentCode string) *hours {
	return &hours{
		instrumentCode: instrumentCode,
		_hours:         make(map[time.Time]bool),
	}
}

func (h *hours) Add(from, to time.Time) {
	from = misc.ToHourUTC(from)
	to = misc.ToHourUTC(to)

	// Ensure order
	if to.Before(from) {
		temp := from
		from = to
		to = temp
	}

	for curr := from; !curr.After(to); curr = curr.Add(time.Hour) {
		h._hours[curr] = true
	}
}

func (h hours) Size() int {
	return len(h._hours)
}

var doNothingListener DownloadListener = func(symbol string, dayHour time.Time, err error, curr, count int) {
	// Do nothing. This is a substitution when listener is passed as nil in Download
}

func (h *hours) Download(downloader *bi5.Downloader, symbol string, progress, count int, listener DownloadListener) int {
	if listener == nil {
		listener = doNothingListener
	}

	for t := range h._hours {
		err := downloader.Download(symbol, t)
		progress++
		listener(symbol, t, err, progress, count)
	}
	return progress
}

type downloaderImpl struct {
	instruments map[string]*hours
	count       int
	downloader  *bi5.Downloader
}

func NewTickDownloader(folder string) TickDownloader {
	return &downloaderImpl{
		downloader:  bi5.NewDownloader(folder),
		instruments: make(map[string]*hours),
	}
}

func (d *downloaderImpl) Add(instrument *instrument.Metadata, from, to time.Time) TickDownloader {
	instrumentCode := instrument.Code()
	h, ok := d.instruments[instrumentCode]
	if !ok {
		h = newHours(instrumentCode)
		d.instruments[instrumentCode] = h
	}
	d.count -= h.Size()
	h.Add(from, to)
	d.count += h.Size()

	return d
}

func (d downloaderImpl) Count() int {
	return d.count
}

func (d downloaderImpl) Download(listener DownloadListener) {
	progress := 0
	for s, i := range d.instruments {
		progress = i.Download(d.downloader, s, progress, d.count, listener)
	}
}
