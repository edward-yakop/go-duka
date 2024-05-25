package fxt4

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/edward-yakop/go-duka/api/instrument"
	"github.com/edward-yakop/go-duka/api/tickdata"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
)

// FxtFile define fxt file format
//
// Refer: https://github.com/EA31337/MT-Formats
// FXT file should be placed in the tester/history directory. name format is SSSSSSPP_M.fxt where:
//
//	SSSSSS - symbol name same as in symbol field in the header
//	PP - timeframe period must be correspond with period field in the header
//	M - model number (0,1 or 2)
type FxtFile struct {
	fpath          string
	instrument     *instrument.Metadata
	model          uint32
	header         *FXTHeader
	firstUniBar    *FxtTick
	lastUniBar     *FxtTick
	deltaTimestamp uint32
	endTimestamp   uint32
	timeframe      uint32
	barCount       int32
	tickCount      int64
	chTicks        chan *FxtTick
	chClose        chan struct{}
}

// NewFxtFile create an new fxt file instance
func NewFxtFile(timeframe, spread, model uint32, dest string, instrument *instrument.Metadata) *FxtFile {
	fn := fmt.Sprintf("%s%d_%d.fxt", instrument.Code(), timeframe, model)
	fxt := &FxtFile{
		header:         NewHeader(405, instrument, timeframe, spread, model),
		fpath:          filepath.Join(dest, fn),
		chTicks:        make(chan *FxtTick, 1024),
		chClose:        make(chan struct{}, 1),
		deltaTimestamp: timeframe * 60,
		timeframe:      timeframe,
		instrument:     instrument,
		model:          model,
	}

	go fxt.worker()
	return fxt
}

func (f *FxtFile) worker() error {
	defer func() {
		close(f.chClose)
		slog.Info("Saved Bar",
			slog.Uint64("timeframe", uint64(f.timeframe)),
			slog.Int64("barCount", int64(f.barCount)),
			slog.Int64("tickCount", f.tickCount),
		)
	}()

	fxt, err := os.OpenFile(f.fpath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	if err != nil {
		slog.Error(
			"Create file failed",
			slog.String("path", f.fpath),
			slog.Any("error", err),
		)

		return err
	}

	defer fxt.Close()
	bu := bytes.NewBuffer(make([]byte, 0, headerSize))

	//
	// convert FXT header
	//
	if err = binary.Write(bu, binary.LittleEndian, f.header); err != nil {
		slog.Error("Convert FXT header failed", slog.Any("error", err))

		return err
	}
	// write FXT file
	if _, err := fxt.Write(bu.Bytes()); err != nil {
		slog.Error("Write FXT header failed", slog.Any("error", err))

		return err
	}

	for tick := range f.chTicks {

		if tick.BarTimestamp > uint64(tick.TickTimestamp) {
			slog.Info("Tick", slog.Any("tick", tick))
		}

		bu.Reset()
		//
		//  write tick data
		//
		if err = binary.Write(bu, binary.LittleEndian, tick); err != nil {
			slog.Error("Pack tick failed", slog.Any("error", err))

			break
		}
		if _, err = fxt.Write(bu.Bytes()); err != nil {
			slog.Error(
				"Write fxt tick failed",
				slog.String("content", bu.String()),
				slog.Any("error", err),
			)

			break
		}

		if f.firstUniBar == nil {
			f.firstUniBar = tick
		}
		f.lastUniBar = tick
	}
	return err
}

func (f *FxtFile) PackTicks(barTimestemp uint32, ticks []*tickdata.TickData) error {

	if len(ticks) == 0 {
		return nil
	}

	var (
		op = ticks[0].Bid
		hi = ticks[0].Bid
		lo = ticks[0].Bid
		vo = math.Max(ticks[0].VolumeBid, 1)
	)

	for _, tick := range ticks {
		ft := &FxtTick{
			BarTimestamp:  uint64(barTimestemp),
			TickTimestamp: uint32(tick.Timestamp / 1000),
			Open:          op,
			High:          math.Max(tick.Bid, hi),
			Low:           math.Min(tick.Bid, lo),
			Close:         tick.Bid,
			Volume:        uint64(math.Max(tick.VolumeBid*100, 1)),
			LaunchExpert:  3,
			//RealSpread:    uint32(tick.Ask - tick.Bid/f.header.PointSize),
		}
		vo = vo + tick.VolumeBid
		f.chTicks <- ft
		f.tickCount++
	}
	if f.endTimestamp != barTimestemp {
		f.barCount++
		f.endTimestamp = barTimestemp
	}
	return nil
}

func (f *FxtFile) adjustHeader() error {
	if f.barCount == 0 {
		return nil
	}

	fxt, err := os.OpenFile(f.fpath, os.O_RDWR, 0666)
	if err != nil {
		slog.Error(
			"Open file %s failed",
			slog.String("path", f.fpath),
			slog.Any("error", err),
		)

		return err
	}
	defer fxt.Close()

	// first part
	if _, err := fxt.Seek(216, os.SEEK_SET); err == nil {
		d := struct {
			BarCount          int32  // Total bar count
			BarStartTimestamp uint32 // Modelling start date - date of the first tick.
			BarEndTimestamp   uint32 // Modelling end date - date of the last tick.
		}{
			f.barCount,
			uint32(f.firstUniBar.BarTimestamp),
			uint32(f.lastUniBar.BarTimestamp),
		}

		bu := new(bytes.Buffer)
		if err = binary.Write(bu, binary.LittleEndian, &d); err == nil {
			_, err = fxt.Write(bu.Bytes())
		}
		if err != nil {
			slog.Error("Adjust FXT header 1 failed", slog.Any("error", err))

			return err
		}
	} else {
		slog.Error("File seek 1 failed", slog.Any("error", err))

		return err
	}

	// end part
	if _, err := fxt.Seek(472, os.SEEK_SET); err == nil {
		d := struct {
			BarStartTimestamp uint32 // Tester start date - date of the first tick.
			BarEndTimestamp   uint32 // Tester end date - date of the last tick.
		}{
			uint32(f.firstUniBar.BarTimestamp),
			uint32(f.lastUniBar.BarTimestamp),
		}
		bu := new(bytes.Buffer)
		if err = binary.Write(bu, binary.LittleEndian, &d); err == nil {
			_, err = fxt.Write(bu.Bytes())
		}
		if err != nil {
			slog.Error("Adjust FXT header 2 failed", slog.Any("error", err))

			return err
		}
	} else {
		slog.Error("File seek 2 failed", slog.Any("error", err))

		return err
	}

	return nil
}

func (f *FxtFile) Finish() error {
	close(f.chTicks)
	<-f.chClose
	return f.adjustHeader()
}

// DumpFile dump fxt file into txt format
func DumpFile(fname string, header bool, w io.Writer) {
	fh, err := os.OpenFile(fname, os.O_RDONLY, 0666)
	if err != nil {
		slog.Error("Open fxt file failed", slog.Any("error", err))
		return
	}
	defer fh.Close()

	bs := make([]byte, headerSize)
	n, err := fh.Read(bs[:])
	if err != nil || n != headerSize {
		slog.Error("Read fxt header failed", slog.Any("error", err))

		return
	}

	var h FXTHeader
	err = binary.Read(bytes.NewBuffer(bs[:]), binary.LittleEndian, &h)
	if err != nil {
		slog.Error("Decode fxt header failed", slog.Any("error", err))

		return
	}

	if w == nil {
		w = os.Stdout
	}
	bw := bufio.NewWriter(w)
	_, _ = bw.WriteString(fmt.Sprintf("Header: %+v\n", h))
	defer bw.Flush()

	if header {
		// only header
		return
	}

	tickBs := make([]byte, tickSize)
	for {
		n, err = fh.Read(tickBs[:tickSize])
		if err == io.EOF {
			break
		}

		if n != tickSize || err != nil {
			slog.Error("Read tick data failed", slog.Any("error", err))

			break
		}

		var tick FxtTick
		err = binary.Read(bytes.NewBuffer(tickBs[:]), binary.LittleEndian, &tick)
		if err != nil {
			slog.Error("Decode tick data failed", slog.Any("error", err))

			break
		}

		_, _ = bw.WriteString(fmt.Sprintf("%s\n", &tick))
	}
}
