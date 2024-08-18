package instrument

import (
	"encoding/json"
	"fmt"
	"github.com/go-resty/resty/v2"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

// This file is a port of https://github.com/Leo4815162342/dukascopy-tools/blob/master/packages/dukascopy-node/src/config/instruments-metadata.ts

type Metadata struct {
	code              string
	name              string
	description       string
	minStartDate      time.Time
	decimalFactor     float64
	minStartDateDaily time.Time

	priceFormat string
}

func (m *Metadata) Code() string {
	if m == nil {
		return ""
	}

	return m.code
}

func (m *Metadata) Name() string {
	if m == nil {
		return ""
	}

	return m.name
}

func (m *Metadata) Description() string {
	if m == nil {
		return ""
	}

	return m.description
}

func (m *Metadata) MinStartDate() time.Time {
	if m == nil {
		return time.Time{}
	}

	return m.minStartDate
}

func (m *Metadata) DecimalFactor() float64 {
	if m == nil {
		return 0
	}

	return m.decimalFactor
}

func (m *Metadata) MinStartDateDaily() time.Time {
	if m == nil {
		return time.Time{}
	}

	return m.minStartDateDaily
}

func (m *Metadata) PriceToString(price float64) string {
	if m == nil {
		return ""
	}

	if len(m.priceFormat) == 0 {
		m.priceFormat = "%." + strconv.Itoa(int(math.Log10(m.decimalFactor))) + "f"
	}

	return fmt.Sprintf(m.priceFormat, price)
}

func (m *Metadata) DiffInPips(openPrice, closePrice string) string {
	if m == nil {
		return ""
	}

	op, opErr := strconv.ParseFloat(openPrice, 64)
	cp, cpErr := strconv.ParseFloat(closePrice, 64)
	if opErr != nil || cpErr != nil {
		return ""
	}

	o := int(op * m.decimalFactor)
	c := int(cp * m.decimalFactor)

	diff := c - o

	return strconv.Itoa(diff)
}

var codeToInstrument = map[string]*Metadata{}
var nameToInstrument = map[string]*Metadata{}

var s sync.RWMutex

// GetMetadata returns instrument with requested code.
// Returns nil if not found
func GetMetadata(code string) *Metadata {
	LoadMetadataFromJson(false)

	s.RLock()
	r := codeToInstrument[strings.ToUpper(code)]
	s.RUnlock()

	return r
}

func GetMetadataByName(name string) *Metadata {
	LoadMetadataFromJson(false)

	s.RLock()
	r := nameToInstrument[name]
	s.RUnlock()

	return r
}

type Instrument struct {
	Name                       string    `json:"name"`
	Description                string    `json:"description"`
	DecimalFactor              int       `json:"decimalFactor"`
	StartHourForTicks          time.Time `json:"startHourForTicks"`
	StartDayForMinuteCandles   time.Time `json:"startDayForMinuteCandles"`
	StartMonthForHourlyCandles time.Time `json:"startMonthForHourlyCandles"`
	StartYearForDailyCandles   time.Time `json:"startYearForDailyCandles"`
}

var URL = "https://raw.githubusercontent.com/Leo4815162342/dukascopy-node/master/src/utils/instrument-meta-data/generated/instrument-meta-data.json"

func LoadMetadataFromJson(isForce bool) {
	if len(codeToInstrument) > 0 && !isForce {
		return
	}

	resp, getErr := resty.New().R().
		SetDoNotParseResponse(true).
		Get(URL)

	if getErr != nil {
		slog.Warn(
			"Failed to retrieve dukas instrument",
			slog.String("url", URL),
			slog.Any("error", getErr),
		)

		return
	}

	if resp.IsError() {
		statusCode := resp.RawResponse.StatusCode
		slog.Error(
			"failed to retrieve metadata",
			slog.Int("httpStatusCode", statusCode),
			slog.String("rawResponse", string(resp.Body())),
		)

		return
	}

	tCodeToInstrument := map[string]*Metadata{}
	tNameToInstrument := map[string]*Metadata{}

	instruments := map[string]Instrument{}
	if unmarshalErr := json.NewDecoder(resp.RawBody()).Decode(&instruments); unmarshalErr != nil {
		slog.Error(
			"failed to unmarshal dukas instrument",
			slog.Any("error", unmarshalErr),
			slog.String("url", URL),
		)

		return
	}
	for instrumentCode, instrument := range instruments {
		metadata := jsonToMetadata(instrumentCode, instrument)
		tCodeToInstrument[metadata.Code()] = metadata
		tNameToInstrument[metadata.Name()] = metadata
	}

	s.Lock()
	codeToInstrument = tCodeToInstrument
	nameToInstrument = tNameToInstrument
	s.Unlock()
}

func jsonToMetadata(code string, instrument Instrument) *Metadata {
	return &Metadata{
		code:              strings.ToUpper(code),
		name:              instrument.Name,
		description:       instrument.Description,
		minStartDate:      instrument.StartDayForMinuteCandles,
		decimalFactor:     float64(instrument.DecimalFactor),
		minStartDateDaily: instrument.StartYearForDailyCandles,
	}
}
