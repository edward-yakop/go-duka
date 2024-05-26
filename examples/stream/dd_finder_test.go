package stream

import (
	"fmt"
	"github.com/edward-yakop/go-duka/api/instrument"
	"github.com/edward-yakop/go-duka/api/tickdata"
	"github.com/edward-yakop/go-duka/api/tickdata/stream"
	"github.com/edward-yakop/go-duka/internal/misc"
	"github.com/stretchr/testify/assert"
	"log/slog"
	"math"
	"strconv"
	"testing"
	"time"
)

func Test_StreamExample_DDFinder(t *testing.T) {
	misc.SetDefaultLog(slog.LevelDebug)

	loc, _ := time.LoadLocation("EET")

	im := instrument.GetMetadata("GBPJPY")
	openTime := time.Date(2020, time.November, 3, 17, 0, 0, 0, loc)
	openPrice := 136.325
	closeTime := time.Date(2020, time.November, 4, 00, 56, 56, 0, loc)
	closePrice := 136.725

	openPriceDiff, maxDD, maxPositive, maxDDForMaxPositive, maxPositiveTime, closePriceDiff :=
		buyDDFinder(t, im, openTime, closeTime, openPrice, closePrice)

	t.Log("Open price diff in [", strconv.Itoa(openPriceDiff), "] points")
	t.Log("Max DD [", strconv.Itoa(maxDD), "] points")
	t.Log("Max Positive [", strconv.Itoa(maxPositive), "] points")
	t.Log("Max Positive Time [", fmtTime(maxPositiveTime), "] Duration [", fmtDuration(maxPositiveTime.Sub(openTime)), "]")
	t.Log("Max DD for Max Positive [", strconv.Itoa(maxDDForMaxPositive), "] points")
	t.Log("Close price diff in [", strconv.Itoa(closePriceDiff), "] points")
	profitInPoints := int(math.Round((closePrice - openPrice) * 1000))
	t.Log("Profit [", strconv.Itoa(profitInPoints), "] points Duration [", fmtDuration(closeTime.Sub(openTime)), "]")

	// Asserts
	assert.Equal(t, 0, openPriceDiff, "openPriceDiff")
	assert.Equal(t, -240, maxDD, "maxDD")
	assert.Equal(t, 392, maxPositive, "maxPositive")
	assert.Equal(t, -240, maxDDForMaxPositive, "maxDDForMaxPositive")
	assert.Equal(t, 400, profitInPoints, "profitInPoints")
}

func buyDDFinder(t *testing.T, instrument *instrument.Metadata, openTime time.Time, closeTime time.Time, openPrice float64, closePrice float64) (
	openPriceDiff int, maxDD int, maxPositive int, maxDDForMaxPositive int, maxPositiveTime time.Time, closePriceDiff int,
) {
	start := openTime.Add(-1 * time.Minute)
	end := closeTime.Add(time.Minute)
	s := stream.New(instrument, start, end, ".")

	maxDD = math.MaxInt32
	openPriceDiff = math.MaxInt32
	closePriceDiff = math.MaxInt32
	maxPositive = math.MinInt32
	s.EachTick(func(tickTime time.Time, tick *tickdata.TickData, err error) bool {
		if openTime.Sub(tickTime) > 0 {
			return true
		}

		if openPriceDiff == math.MaxInt32 {
			logTick(t, "open", tickTime, tick)
			openPriceDiff = int(math.Round((openPrice - tick.Ask) * 1000))
		}

		dd := math.Round((tick.Bid - openPrice) * 1000)
		maxDD = int(math.Min(float64(maxDD), dd))

		ddInInt := int(dd)
		if maxPositive <= ddInInt {
			maxPositive = ddInInt
			maxPositiveTime = tickTime
			maxDDForMaxPositive = maxDD
		}

		if closeTime.Sub(tickTime) <= 0 && closePriceDiff == math.MaxInt32 {
			logTick(t, "close", tickTime, tick)
			closePriceDiff = int(math.Round((tick.Bid - closePrice) * 1000))
			return false
		}

		return true
	})
	return openPriceDiff, maxDD, maxPositive, maxDDForMaxPositive, maxPositiveTime, closePriceDiff
}

func logTick(t *testing.T, op string, tickTime time.Time, tick *tickdata.TickData) {
	if tick == nil {
		return
	}

	t.Helper()
	timestamp := tick.Timestamp
	ask := tick.Ask
	bid := tick.Bid

	t.Logf("%s date [%s] timestamp [%d], ask [%.3f] bid [%.3f]", op, fmtTime(tickTime), timestamp, ask, bid)
}

func fmtDuration(d time.Duration) string {
	d = d.Round(time.Minute)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

func fmtTime(tickTime time.Time) string {
	return tickTime.Format("2006-01-02 15:04:05")
}
