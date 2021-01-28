package downloader

import (
	"github.com/ed-fx/go-duka/api/instrument"
	"github.com/ed-fx/go-duka/internal/bi5"
	"github.com/ed-fx/go-duka/internal/misc"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestDownloader(t *testing.T) {
	folder := createEmptyDir(t)

	d := NewTickDownloader(folder)
	assert.NotNil(t, d)

	from := time.Date(2021, time.January, 8, 0, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	d.Add(instrument.GetMetadata("EURUSD"), from, to).
		Add(instrument.GetMetadata("GBPUSD"), from, to)

	d.Download(func(instrumentCode string, dayHour time.Time, err error, curr, count int) {

	})

	fileExists(t, bi5.BiFilePathTime(folder, "EURUSD", from))
	fileExists(t, bi5.BiFilePathTime(folder, "EURUSD", to))
	fileExists(t, bi5.BiFilePathTime(folder, "GBPUSD", from))
	fileExists(t, bi5.BiFilePathTime(folder, "GBPUSD", to))
}

func fileExists(t *testing.T, filePath string) {
	assert.True(t, misc.IsFileExists(filePath))
}

func createEmptyDir(t *testing.T) string {
	dir, err := ioutil.TempDir(".", "test")
	assert.NoError(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})
	return dir
}
