package hst

import (
	"fmt"
	"github.com/edward-yakop/go-duka/api/instrument"
	"github.com/edward-yakop/go-duka/api/tickdata"
	"log/slog"
	"math"
	"os"
	"path/filepath"
)

// HST401 MT4 history data format .hst with version 401
type HST401 struct {
	header     *Header
	dest       string
	instrument *instrument.Metadata
	spread     uint32
	timefame   uint32
	barCount   int64
	chBars     chan *BarData
	chClose    chan struct{}
}

// NewHST create a HST convertor
func NewHST(timefame, spread uint32, instrument *instrument.Metadata, dest string) *HST401 {
	hst := &HST401{
		header:     NewHeader(timefame, instrument),
		dest:       dest,
		instrument: instrument,
		spread:     spread,
		timefame:   timefame,
		chBars:     make(chan *BarData, 128),
		chClose:    make(chan struct{}, 1),
	}

	go hst.worker()
	return hst
}

// worker goroutine which flust data to disk
func (h *HST401) worker() error {
	fname := fmt.Sprintf("%s%d.hst", h.instrument.Code(), h.timefame)
	fpath := filepath.Join(h.dest, fname)

	f, err := os.OpenFile(fpath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	if err != nil {
		slog.Error(
			"Failed to create file",
			slog.String("path", fpath),
			slog.Any("error", err),
		)

		return err
	}

	defer func() {
		_ = f.Close()
		close(h.chClose)

		slog.Info("Saved Bar",
			slog.Uint64("timeframe", uint64(h.timefame)),
			slog.Int64("barcount", h.barCount),
		)
	}()

	// write HST header
	var bs []byte

	if bs, err = h.header.ToBytes(); err != nil {
		slog.Error(
			"Pack HST Header failed",
			slog.Any("header", h.header),
			slog.Any("error", err),
		)

		return err
	}
	if _, err = f.Write(bs[:]); err != nil {
		slog.Error("Write HST Header failed",
			slog.Any("header", h.header),
			slog.Any("error", err),
		)

		return err
	}

	for bar := range h.chBars {
		if bs, err = bar.ToBytes(); err == nil {
			if _, err = f.Write(bs[:]); err != nil {
				slog.Error(
					"Write Bardata failed",
					slog.Any("bar", bar),
					slog.Any("error", err),
				)
			}
		} else {
			slog.Error("Pack BarData failed",
				slog.Any("error", err),
				slog.Any("bar", bar),
			)

			continue
		}
	}

	if err != nil {
		slog.Warn(
			"HST worker return with",
			slog.Any("error", err),
		)
	}
	return err
}

// PackTicks aggregate ticks with timeframe
func (h *HST401) PackTicks(barTimestamp uint32, ticks []*tickdata.TickData) error {
	// Transform universal bar list to binary bar data (60 Bytes per bar)
	if len(ticks) == 0 {
		return nil
	}

	bar := &BarData{
		CTM:   uint64(barTimestamp), //uint32(ticks[0].Timestamp / 1000),
		Open:  ticks[0].Bid,
		Low:   ticks[0].Bid,
		High:  ticks[0].Bid,
		Close: ticks[0].Bid,
	}

	var totalVol float64
	for _, tick := range ticks {
		bar.Close = tick.Bid
		bar.Low = math.Min(tick.Bid, bar.Low)
		bar.High = math.Max(tick.Bid, bar.High)
		totalVol = totalVol + tick.VolumeBid /*+tick.VolumeAsk*/
	}
	bar.Volume = uint64(math.Max(totalVol, 1))

	select {
	case h.chBars <- bar:
		//log.Trace("Bar %d: %v.", h.barCount, bar)
		h.barCount++
		break
		//case <-h.close:
		//	break
	}
	return nil
}

// Finish HST file convert
func (h *HST401) Finish() error {
	close(h.chBars)
	<-h.chClose
	return nil
}
