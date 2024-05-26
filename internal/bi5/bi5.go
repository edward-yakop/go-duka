package bi5

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"github.com/edward-yakop/go-duka/api/instrument"
	"github.com/edward-yakop/go-duka/api/tickdata"
	"github.com/edward-yakop/go-duka/internal/misc"
	"github.com/pkg/errors"
	"io"
	"os"
	"time"

	"github.com/edward-yakop/go-duka/internal/core"
	"github.com/ulikunitz/xz/lzma"
)

const ext = "bi5"

var httpDownload = core.NewDownloader()

const (
	TICK_BYTES = 20
)

// Bi5 from dukascopy
type Bi5 struct {
	dayHour        time.Time
	endDayHour     time.Time
	metadata       *instrument.Metadata
	targetFilePath string
	save           bool
	downloader     *Downloader
}

func (b Bi5) DayHour() time.Time {
	return b.dayHour
}

func (b Bi5) InstrumentCode() string {
	return b.metadata.Code()
}

// New create an bi5 saver
func New(dayHour time.Time, metadata *instrument.Metadata, downloadFolderPath string) *Bi5 {
	dayHour = dayHour.UTC()
	y, m, d := dayHour.Date()

	beginHour := time.Date(y, m, d, dayHour.Hour(), 0, 0, 0, time.UTC)
	endHour := beginHour.Add(time.Hour).Add(-1)
	return &Bi5{
		targetFilePath: BiFilePath(downloadFolderPath, metadata.Code(), y, int(m), d, dayHour.Hour()),
		dayHour:        beginHour,
		endDayHour:     endHour,
		metadata:       metadata,
		downloader:     NewDownloader(downloadFolderPath),
	}
}

type TickDataResult struct {
	Tick  *tickdata.TickData
	Error error
}

func (b Bi5) Ticks() ([]*tickdata.TickData, error) {
	return b.TicksBetween(time.Time{}, time.Time{})
}

func (b Bi5) TicksBetween(from time.Time, to time.Time) (r []*tickdata.TickData, err error) {
	r = make([]*tickdata.TickData, 0)

	location := b.dayHour.Location()
	from, err = b.sanitizeFrom(from, location)
	if err != nil {
		return
	}
	to, err = b.sanitizeTo(to, location)
	if err != nil {
		return
	}

	b.EachTick(func(tick *tickdata.TickData, terr error) (isContinue bool) {
		if terr == nil {
			t := tick.UTC()
			if !(t.Before(from) || t.After(to)) {
				r = append(r, tick)
			}
			isContinue = to.After(t)
		} else {
			err = terr
		}

		return
	})

	if err != nil {
		r = []*tickdata.TickData{}
	}
	return
}

func (b Bi5) sanitizeFrom(from time.Time, location *time.Location) (time.Time, error) {
	if from.IsZero() {
		return b.dayHour, nil
	}
	fromSanitize := from.In(location)
	if fromSanitize.Before(b.dayHour) || fromSanitize.Equal(b.dayHour) {
		return b.dayHour, nil
	}
	if fromSanitize.Before(b.endDayHour) || fromSanitize.Equal(b.endDayHour) {
		return fromSanitize, nil
	}

	return time.Time{}, errors.New("From [" + from.String() + "] is after [" + b.endDayHour.String() + "]")
}

func (b Bi5) sanitizeTo(to time.Time, location *time.Location) (time.Time, error) {
	if to.IsZero() {
		return b.endDayHour, nil
	}
	toSanitize := to.In(location)
	if toSanitize.Before(b.dayHour) || toSanitize.Equal(b.dayHour) {
		return time.Time{}, errors.New("To [" + to.String() + "] is before [" + b.dayHour.String() + "]")
	}

	return toSanitize, nil
}

// decodeTickData from input data bytes array.
// the valid data array should be at size `TICK_BYTES`.
//
//	struck.unpack(!IIIff)
//	date, ask / point, bid / point, round(volume_ask * 100000), round(volume_bid * 100000)
func (b Bi5) decodeTickData(data []byte, symbol string, timeH time.Time) (*tickdata.TickData, error) {
	if len(data) != TICK_BYTES {
		return nil, errors.New("invalid length for tick data")
	}

	var err error
	var timeMs, ask, bid int32
	var volumeAsk, volumeBid float32

	buf := bytes.NewBuffer(data)
	timeMs, err = read[int32](err, buf, "time")
	ask, err = read[int32](err, buf, "ask")
	bid, err = read[int32](err, buf, "bid")
	volumeAsk, err = read[float32](err, buf, "volumeAsk")
	volumeBid, err = read[float32](err, buf, "volumeBid")

	var point = b.metadata.DecimalFactor()

	t := tickdata.TickData{
		Symbol:    symbol,
		Timestamp: timeH.Unix()*1000 + int64(timeMs), //timeH.Add(time.Duration(raw.TimeMs) * time.Millisecond),
		Ask:       float64(ask) / point,
		Bid:       float64(bid) / point,
		VolumeAsk: float64(volumeAsk),
		VolumeBid: float64(volumeBid),
	}

	return &t, nil
}

func read[T any](existingErr error, buf *bytes.Buffer, field string) (r T, err error) {
	if existingErr != nil {
		err = existingErr

		return
	}

	err = binary.Read(buf, binary.BigEndian, &r)
	if err != nil {
		err = errors.Wrapf(err, "failed to read field [%s]", field)
	}

	return r, err
}

// Download from dukascopy
func (b Bi5) Download() error {
	return b.downloader.Download(b.InstrumentCode(), b.dayHour)
}

func (b Bi5) EachTick(it tickdata.TickIterator) {
	if !misc.IsFileExists(b.targetFilePath) {
		return
	}

	f, err := os.OpenFile(b.targetFilePath, os.O_RDONLY, 0666)
	if err != nil {
		err = errors.Wrap(err, "Failed to open "+b.targetFilePath+"]")
		it(nil, err)
		return
	}

	defer func(f *os.File) { _ = f.Close() }(f)

	reader, lzmaErr := lzma.NewReader(bufio.NewReader(f))
	if lzmaErr != nil {
		err = errors.Wrapf(err, "failed to create file [%s] reader", b.targetFilePath)
		it(nil, err)

		return
	}

	bytesArr := make([]byte, TICK_BYTES)
	var bytesCount = 0
	var tick *tickdata.TickData
	for {
		tick = nil
		bytesCount, err = reader.Read(bytesArr[:])
		if err == io.EOF {
			err = nil

			break
		}

		if bytesCount != TICK_BYTES || err != nil {
			err = errors.Wrapf(err, "LZMA decode failed: [%d] for file [%s]", bytesCount, b.targetFilePath)
		} else {
			tick, err = b.decodeTickData(bytesArr[:], b.InstrumentCode(), b.dayHour)
			if err != nil {
				err = errors.Wrapf(err, "decode tick data failed for file [%s]", b.targetFilePath)
			}
		}

		if !it(tick, err) {
			return
		}
	}
}
