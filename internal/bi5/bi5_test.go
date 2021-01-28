package bi5

import (
	"github.com/ed-fx/go-duka/api/instrument"
	"github.com/ed-fx/go-duka/api/tickdata"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestBi5DownloadBetween(t *testing.T) {
	from := time.Date(2017, time.January, 10, 22, 0, 0, 0, time.UTC)
	to := from.Add(150 * time.Millisecond)
	testDownloadAndTicks(t, from, to, "1484085600088 1.05549 1.05548 0.75 0.75")

	to = from.Add(time.Hour)
	testDownloadAndTicks(t, from, to, "1484085600088 1.05549 1.05548 0.75 0.75")
}

func testDownloadAndTicks(t *testing.T, from time.Time, to time.Time, expectedFirstTickDataString string) (ticks []*tickdata.TickData) {
	dir := createEmptyDir(t)
	bi := New(from, instrument.GetMetadata("EURUSD"), dir)
	err := bi.Download()
	assert.NoError(t, err)

	ticks, err = bi.TicksBetween(from, to)
	if assert.NoError(t, err) {
		firstTickDataString := ticks[0].StringUnix()
		assert.Equal(t, expectedFirstTickDataString, firstTickDataString)

		for _, tick := range ticks {
			tt := tick.UTC()
			assert.True(t, from.Before(tt) || from.Equal(tt))
			assert.True(t, to.After(tt) || to.Equal(tt))
		}
	} else {
		t.Fail()
	}

	return
}

func createEmptyDir(t *testing.T) string {
	dir, err := ioutil.TempDir(".", "test")
	assert.NoError(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})
	return dir
}
