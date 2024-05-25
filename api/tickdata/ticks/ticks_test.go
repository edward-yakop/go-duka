package ticks

import (
	"github.com/edward-yakop/go-duka/api/instrument"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
	"time"
	_ "time/tzdata" // Ensure that custom timezone is included
)

func TestTick_IncludingWeekendAndGoto(t *testing.T) {
	start := time.Date(2017, time.January, 6, 21, 0, 0, 0, time.UTC)
	end := time.Date(2017, time.January, 8, 22, 59, 0, 0, time.UTC)
	ticks := New(instrument.GetMetadata("GBPJPY"), start, end, createEmptyDir(t))

	var isSkip = true
	for {
		isSuccess, nErr := ticks.Next()
		assert.NoError(t, nErr)

		if !ticks.IsCompleted() {
			assert.True(t, isSuccess)
			tick := ticks.Current()

			assert.NotNil(t, tick)
			assertTime(t, tick.UTC(), start, end)
			break
		} else {
			assert.False(t, isSuccess)
		}

		if isSkip {
			nextHour := start.Add(time.Hour)
			isSuccess, nErr = ticks.Goto(nextHour)
			assert.True(t, isSuccess)
			assert.NoError(t, nErr)

			// Confirm that the
			tickTime := ticks.Current().UTC()
			assert.Equal(t, nextHour.Year(), tickTime.Year())
			assert.Equal(t, nextHour.Month(), tickTime.Month())
			assert.Equal(t, nextHour.Day(), tickTime.Day())
			assert.Equal(t, nextHour.Hour(), tickTime.Hour())
			assert.Equal(t, nextHour.Minute(), tickTime.Minute())

			isSkip = false
		}
	}
}

func createEmptyDir(t *testing.T) string {
	dir, err := ioutil.TempDir(".", "test")
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})
	return dir
}

func assertTime(t *testing.T, time time.Time, start time.Time, end time.Time) {
	if !assert.True(t, start.Before(time) || start.Equal(time)) {
		t.FailNow()
	}
	if !assert.True(t, end.Equal(time) || end.After(time)) {
		t.FailNow()
	}
}
