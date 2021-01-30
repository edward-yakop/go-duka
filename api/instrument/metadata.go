package instrument

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
	log "unknwon.dev/clog/v2"
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

func (m Metadata) Code() string {
	return m.code
}

func (m Metadata) Name() string {
	return m.name
}

func (m Metadata) Description() string {
	return m.description
}

func (m Metadata) MinStartDate() time.Time {
	return m.minStartDate
}

func (m Metadata) DecimalFactor() float64 {
	return m.decimalFactor
}

func (m Metadata) MinStartDateDaily() time.Time {
	return m.minStartDateDaily
}

func (m *Metadata) PriceToString(price float64) string {
	if len(m.priceFormat) == 0 {
		m.priceFormat = "%." + strconv.Itoa(int(math.Log10(m.decimalFactor))) + "f"
	}
	return fmt.Sprintf(m.priceFormat, price)
}

var codeToInstrument map[string]*Metadata = nil
var nameToInstrument map[string]*Metadata = nil

// Returns instrument with requested code.
//
// Returns [nil] if not found
func GetMetadata(code string) *Metadata {
	loadMetadataFromJson()
	return codeToInstrument[strings.ToUpper(code)]
}

func GetMetadataByName(name string) *Metadata {
	loadMetadataFromJson()
	return nameToInstrument[name]
}

type InstrumentJson struct {
	Name                       string    `json:"name"`
	Description                string    `json:"description"`
	DecimalFactor              int       `json:"decimalFactor"`
	StartHourForTicks          time.Time `json:"startHourForTicks"`
	StartDayForMinuteCandles   time.Time `json:"startDayForMinuteCandles"`
	StartMonthForHourlyCandles time.Time `json:"startMonthForHourlyCandles"`
	StartYearForDailyCandles   time.Time `json:"startYearForDailyCandles"`
}

func loadMetadataFromJson() {
	if codeToInstrument != nil {
		return
	}

	codeToInstrument = make(map[string]*Metadata)
	nameToInstrument = make(map[string]*Metadata)

	const url = "https://raw.githubusercontent.com/Leo4815162342/dukascopy-tools/master/packages/dukascopy-node/src/utils/instrument-meta-data/generated/instrument-meta-data.json"
	resp, err := http.Get(url)
	if err != nil {
		log.Warn("Failed to retrieve dukas instrument from [" + url + "]")
	}

	defer resp.Body.Close()

	var m map[string]InstrumentJson
	json.NewDecoder(resp.Body).Decode(&m)
	for instrumentCode, instrument := range m {
		metadata := jsonToMetadata(instrumentCode, instrument)
		codeToInstrument[metadata.Code()] = metadata
		nameToInstrument[metadata.Name()] = metadata
	}
}

func jsonToMetadata(code string, instrument InstrumentJson) *Metadata {
	return &Metadata{
		code:              strings.ToUpper(code),
		name:              instrument.Name,
		description:       instrument.Description,
		minStartDate:      instrument.StartDayForMinuteCandles,
		decimalFactor:     float64(instrument.DecimalFactor),
		minStartDateDaily: instrument.StartYearForDailyCandles,
	}
}
