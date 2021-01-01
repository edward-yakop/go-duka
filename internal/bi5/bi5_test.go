package bi5

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestBi5DownloadAndStream(t *testing.T) {
	dateHour := time.Date(2017, time.January, 10, 22, 0, 0, 0, time.UTC)
	testDownloadAndStream(t, dateHour, "1484085600088 1.05549 1.05548 0.75 0.75")
}

func testDownloadAndStream(t *testing.T, dateHour time.Time, firstTickDataString string) {
	dir := createEmptyDir(t)
	bi := New(dateHour, "EURUSD", dir)
	err := bi.Download()
	assert.NoError(t, err)

	var isFirst = true
	ticks, err := bi.Ticks()
	if assert.NoError(t, err) {
		for _, tick := range ticks {
			tickDataString := tick.StringUnix()
			if isFirst {
				assert.Equal(t, firstTickDataString, tickDataString)
				isFirst = false
			}
			//fmt.Printf("%v\n", tickDataString)
		}
	} else {
		t.Fail()
	}
}

func createEmptyDir(t *testing.T) string {
	dir, err := ioutil.TempDir(".", "test")
	assert.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})
	return dir
}
