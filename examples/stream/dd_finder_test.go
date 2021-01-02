package stream

import (
	"fmt"
	"github.com/ed-fx/go-duka/api/tickdata"
	"github.com/ed-fx/go-duka/api/tickdata/stream"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"testing"
	"time"
)

func Test_StreamExample_DDFinder(t *testing.T) {
	loc, _ := time.LoadLocation("EET")

	symbol := "GBPJPY"
	openTime := time.Date(2020, time.November, 3, 17, 0, 0, 0, loc)
	openPrice := 136.325
	closeTime := time.Date(2020, time.November, 4, 00, 56, 56, 0, loc)
	closePrice := 136.725

	openPriceDiff, maxDD, maxPositive, maxDDForMaxPositive, maxPositiveTime, closePriceDiff :=
		buyDDFinder(t, symbol, openTime, closeTime, openPrice, closePrice)

	println("Open price diff in [", strconv.Itoa(openPriceDiff), "] points")
	println("Max DD [", strconv.Itoa(maxDD), "] points")
	println("Max Positive [", strconv.Itoa(maxPositive), "] points")
	println("Max Positive Time [", fmtTime(maxPositiveTime), "] Duration [", fmtDuration(maxPositiveTime.Sub(openTime)), "]")
	println("Max DD for Max Positive [", strconv.Itoa(maxDDForMaxPositive), "] points")
	println("Close price diff in [", strconv.Itoa(closePriceDiff), "] points")
	profitInPoints := int(math.Round((closePrice - openPrice) * 1000))
	println("Profit [", strconv.Itoa(profitInPoints), "] points Duration [", fmtDuration(closeTime.Sub(openTime)), "]")

	// Asserts
	assert.Equal(t, 0, openPriceDiff)
	assert.Equal(t, -240, maxDD)
	assert.Equal(t, 392, maxPositive)
	assert.Equal(t, -240, maxDDForMaxPositive)
	assert.Equal(t, 400, profitInPoints)
}

func buyDDFinder(t *testing.T, symbol string, openTime time.Time, closeTime time.Time, openPrice float64, closePrice float64) (
	openPriceDiff int, maxDD int, maxPositive int, maxDDForMaxPositive int, maxPositiveTime time.Time, closePriceDiff int,
) {
	start := openTime.Add(-1 * time.Minute)
	end := closeTime.Add(time.Minute)
	stream := stream.New(symbol, start, end, createEmptyDir(t))

	maxDD = math.MaxInt32
	openPriceDiff = math.MaxInt32
	closePriceDiff = math.MaxInt32
	maxPositive = math.MinInt32
	stream.EachTick(func(tickTime time.Time, tick *tickdata.TickData, err error) bool {
		if openTime.Sub(tickTime) > 0 {
			return true
		}

		if openPriceDiff == math.MaxInt32 {
			printTick(" open", tickTime, tick)
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
			printTick("close", tickTime, tick)
			closePriceDiff = int(math.Round((tick.Bid - closePrice) * 1000))
			return false
		}

		return true
	})
	return openPriceDiff, maxDD, maxPositive, maxDDForMaxPositive, maxPositiveTime, closePriceDiff
}

func createEmptyDir(t *testing.T) string {
	dir, err := ioutil.TempDir(".", "test")
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})
	return dir
}

func printTick(op string, tickTime time.Time, tick *tickdata.TickData) {
	println(op,
		"date [", fmtTime(tickTime), "] timestamp [", tick.Timestamp,
		"] ask [", fmtPrice(tick.Ask), "] bid [", fmtPrice(tick.Bid), "]",
	)
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

func fmtPrice(p float64) string {
	return fmt.Sprintf("%.3f", p)
}
