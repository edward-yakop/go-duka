package stream

import (
	"github.com/ed-fx/go-duka/api/instrument"
	"github.com/ed-fx/go-duka/api/tickdata"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
	"time"
	_ "time/tzdata" // Ensure that custom timezone is included
)

func TestStream_EachTick_AlwaysContinue(t *testing.T) {
	start := time.Date(2017, time.January, 10, 22, 0, 0, 0, time.UTC)
	end := start.Add(1 * time.Hour)
	stream := New(instrument.GetMetadata("GBPJPY"), start, end, createEmptyDir(t))
	isRun := false

	stream.EachTick(func(time time.Time, tick *tickdata.TickData, err error) bool {
		isRun = true
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		assertTime(t, time, start, end)
		assert.NotNil(t, tick)
		return true
	})
	assert.True(t, isRun)
}

func assertTime(t *testing.T, time time.Time, start time.Time, end time.Time) {
	if !assert.True(t, start.Before(time) || start.Equal(time)) {
		t.FailNow()
	}
	if !assert.True(t, end.Equal(time) || end.After(time)) {
		t.FailNow()
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

func TestStream_EachTick_OnlyContinueTwice(t *testing.T) {
	location, err := time.LoadLocation("America/New_York")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	start := time.Date(2017, time.January, 10, 22, 0, 0, 0, location)
	end := start.Add(4 * 24 * time.Hour)
	stream := New(instrument.GetMetadata("GBPJPY"), start, end, createEmptyDir(t))

	isRun := false
	tickCount := 0
	stream.EachTick(func(time time.Time, tick *tickdata.TickData, err error) bool {
		isRun = true
		tickCount++
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		assertTime(t, time, start, end)
		assert.NotNil(t, tick)
		return tickCount < 2
	})
	assert.True(t, isRun)
	assert.Equal(t, 2, tickCount)
}
