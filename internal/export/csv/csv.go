package csv

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"github.com/edward-yakop/go-duka/api/instrument"
	"github.com/edward-yakop/go-duka/api/tickdata"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

var (
	ext       = "CSV"
	csvHeader = []string{"time", "ask", "bid", "ask_volume", "bid_volume"}
)

// CsvDump save csv format
type CsvDump struct {
	day        time.Time
	end        time.Time
	dest       string
	instrument *instrument.Metadata
	header     bool
	tickCount  int64
	chClose    chan struct{}
	chTicks    chan *tickdata.TickData
}

// New Csv file
func New(start, end time.Time, header bool, instrument *instrument.Metadata, dest string) *CsvDump {
	csvDump := &CsvDump{
		day:        start,
		end:        end,
		dest:       dest,
		instrument: instrument,
		header:     header,
		chClose:    make(chan struct{}, 1),
		chTicks:    make(chan *tickdata.TickData, 1024),
	}

	go csvDump.worker()

	return csvDump
}

// Finish complete csv file writing
func (c *CsvDump) Finish() error {
	close(c.chTicks)
	<-c.chClose
	return nil
}

// PackTicks handle ticks data
func (c *CsvDump) PackTicks(barTimestamp uint32, ticks []*tickdata.TickData) error {
	for _, tick := range ticks {
		select {
		case c.chTicks <- tick:
			c.tickCount++
			break
		}
	}
	return nil
}

const dayFormat = "2006-01-02"

// worker goroutine which flush data to disk
func (c *CsvDump) worker() error {
	fname := fmt.Sprintf("%s-%s-%s.%s",
		c.instrument.Code(),
		c.day.Format(dayFormat),
		c.end.Format(dayFormat),
		ext)

	fpath := filepath.Join(c.dest, fname)
	f, err := os.OpenFile(fpath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	if err != nil {
		slog.Error("Failed to create file %s, error %v.", fpath, err)

		return err
	}

	defer func() {
		_ = f.Close()
		close(c.chClose)
		slog.Info("Saved Ticks: %d.", c.tickCount)
	}()

	csvw := csv.NewWriter(bufio.NewWriter(f))
	defer csvw.Flush()

	// write header
	if c.header {
		_ = csvw.Write(csvHeader)
	}

	// write tick one by one
	for tick := range c.chTicks {
		row := c.toRow(tick)
		if err = csvw.Write(row); err != nil {
			slog.Error(
				"Write csv failed",
				slog.Any("error", err),
				slog.String("path", fpath),
			)
			break
		}
	}

	return err
}

func (c CsvDump) toRow(t *tickdata.TickData) []string {
	return []string{
		t.UTC().Format("2006-01-02 15:04:05.000"),
		c.instrument.PriceToString(t.Ask),
		c.instrument.PriceToString(t.Bid),
		fmt.Sprintf("%.2f", t.VolumeAsk),
		fmt.Sprintf("%.2f", t.VolumeBid),
	}
}
