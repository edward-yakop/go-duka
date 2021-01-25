package bi5

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/ed-fx/go-duka/api/instrument"
	"github.com/ed-fx/go-duka/api/tickdata"
	"github.com/pkg/errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/ed-fx/go-duka/internal/core"
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
	symbol         string
	metadata       *instrument.Metadata
	targetFilePath string
	save           bool
}

func (b Bi5) DayHour() time.Time {
	return b.dayHour
}

func (b Bi5) Symbol() string {
	return b.symbol
}

// New create an bi5 saver
func New(dayHour time.Time, symbol, downloadFolderPath string) *Bi5 {
	y, m, d := dayHour.UTC().Date()

	biFilePath := filepath.FromSlash(fmt.Sprintf("%s/download/%s/%04d/%02d/%02d/%02dh_ticks.%s", downloadFolderPath, symbol, y, m, d, dayHour.Hour(), ext))
	metadata := instrument.GetMetadata(symbol)

	beginHour := time.Date(y, m, d, dayHour.Hour(), 0, 0, 0, time.UTC)
	endHour := beginHour.Add(time.Hour).Add(-1)
	return &Bi5{
		targetFilePath: biFilePath,
		dayHour:        beginHour,
		endDayHour:     endHour,
		symbol:         symbol,
		metadata:       metadata,
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
//  struck.unpack(!IIIff)
//  date, ask / point, bid / point, round(volume_ask * 100000), round(volume_bid * 100000)
//
func (b Bi5) decodeTickData(data []byte, symbol string, timeH time.Time) (*tickdata.TickData, error) {
	raw := struct {
		TimeMs    int32 // millisecond offset of current hour
		Ask       int32
		Bid       int32
		VolumeAsk float32
		VolumeBid float32
	}{}

	if len(data) != TICK_BYTES {
		return nil, errors.New("invalid length for tick data")
	}

	buf := bytes.NewBuffer(data)
	if err := binary.Read(buf, binary.BigEndian, &raw); err != nil {
		return nil, err
	}

	var point = b.metadata.DecimalFactor()
	t := tickdata.TickData{
		Symbol:    symbol,
		Timestamp: timeH.Unix()*1000 + int64(raw.TimeMs), //timeH.Add(time.Duration(raw.TimeMs) * time.Millisecond),
		Ask:       float64(raw.Ask) / point,
		Bid:       float64(raw.Bid) / point,
		VolumeAsk: float64(raw.VolumeAsk),
		VolumeBid: float64(raw.VolumeBid),
	}

	return &t, nil
}

// Download from dukascopy
func (b Bi5) Download() error {
	if b.isDownloaded() {
		return nil
	}

	year, month, day := b.dayHour.UTC().Date()
	link := fmt.Sprintf(core.DukaTmplURL, b.symbol, year, month-1, day, b.dayHour.Hour())

	var httpStatusCode int
	httpStatusCode, filesize, err := httpDownload.Download(link, b.targetFilePath)
	if err != nil {
		return errors.Wrap(err, "Failed to download tick data for ["+b.symbolAndTime()+"]")
	}

	if httpStatusCode == http.StatusNotFound {
		notFound := b.targetFilePath + ".notFound"
		err = b.createFile(notFound)
		if err != nil {
			err = errors.Wrap(err, "Failed to create tick data ["+b.symbolAndTime()+"] not found file")
			return err
		}
	}

	if filesize == 0 {
		err = os.Rename(b.targetFilePath, b.targetFilePath+".empty")
		if err != nil {
			return errors.Wrap(err, "Failed to create tick data ["+b.symbolAndTime()+"] empty file")
		}
	}

	return nil
}

func (b Bi5) isDownloaded() bool {
	return b.isFileExists(b.targetFilePath) ||
		b.isFileExists(b.targetFilePath+".empty") ||
		b.isFileExists(b.targetFilePath+".notFound")
}

func (b Bi5) isFileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil || os.IsExist(err)
}

func (b Bi5) symbolAndTime() string {
	return b.symbol + ": " + b.dayHour.Format("2006-01-02:15H")
}

func (b Bi5) createFile(path string) error {
	// Create dir if not exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		err = errors.Wrap(err, "Create folder ["+dir+"] failed")
		return err
	}

	emptyFile, err := os.Create(path)
	if err == nil {
		defer emptyFile.Close()
	}
	return err
}

func (b Bi5) EachTick(it tickdata.TickIterator) {
	if !b.isFileExists(b.targetFilePath) {
		return
	}

	f, err := os.OpenFile(b.targetFilePath, os.O_RDONLY, 0666)
	if err != nil {
		err = errors.Wrap(err, "Failed to open "+b.targetFilePath+"]")
		it(nil, err)
		return
	}

	defer f.Close()

	reader, err := lzma.NewReader(f)
	if err != nil {
		err = errors.Wrap(err, "Failed to create file reader")
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
			err = errors.Wrap(err, "LZMA decode failed: ["+strconv.Itoa(bytesCount)+"] for file ["+b.targetFilePath+"]")
		} else {
			tick, err = b.decodeTickData(bytesArr[:], b.symbol, b.dayHour)
			if err != nil {
				err = errors.Wrap(err, "Decode tick data failed for file ["+b.targetFilePath+"]")
			}
		}

		if !it(tick, err) {
			return
		}
	}
}
