package bi5

import (
	"fmt"
	"github.com/edward-yakop/go-duka/internal/core"
	"github.com/edward-yakop/go-duka/internal/misc"
	"github.com/pkg/errors"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

type Downloader struct {
	folder string
}

func NewDownloader(folder string) *Downloader {
	return &Downloader{
		folder: folder,
	}
}

func (d Downloader) Download(instrumentCode string, t time.Time) error {
	dayHour := misc.ToHourUTC(t)
	year, month, day := dayHour.Date()
	hour := dayHour.Hour()
	targetFilePath := BiFilePath(d.folder, instrumentCode, year, int(month), day, hour)

	if d.isDownloaded(targetFilePath) {
		return nil
	}

	link := fmt.Sprintf(core.DukaTmplURL, instrumentCode, year, month-1, day, hour)

	var httpStatusCode int
	httpStatusCode, filesize, err := httpDownload.Download(link, targetFilePath)
	if err != nil {
		symbolTime := d.symbolAndTime(instrumentCode, dayHour)
		return errors.Wrap(err, "Failed to download tick data for ["+symbolTime+"]")
	}

	if httpStatusCode == http.StatusNotFound {
		notFound := targetFilePath + ".notFound"
		err = d.createFile(notFound)
		if err != nil {
			symbolTime := d.symbolAndTime(instrumentCode, dayHour)
			err = errors.Wrap(err, "Failed to create tick data ["+symbolTime+"] not found file")
			return err
		}
	}

	if filesize == 0 {
		err = os.Rename(targetFilePath, targetFilePath+".empty")
		if err != nil {
			symbolTime := d.symbolAndTime(instrumentCode, dayHour)
			return errors.Wrap(err, "Failed to create tick data ["+symbolTime+"] empty file")
		}
	}

	return nil
}

func (d Downloader) isDownloaded(targetFile string) bool {
	return misc.IsFileExists(targetFile) ||
		misc.IsFileExists(targetFile+".empty") ||
		misc.IsFileExists(targetFile+".notFound")
}

func (d Downloader) symbolAndTime(symbol string, dayHour time.Time) string {
	return symbol + ": " + dayHour.Format("2006-01-02:15H")
}

func (d Downloader) createFile(path string) error {
	// Create dir if not exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		err = errors.Wrap(err, "Create folder ["+dir+"] failed")

		return err
	}

	emptyFile, err := os.Create(path)
	if err == nil {
		defer func(emptyFile *os.File) {
			_ = emptyFile.Close()
		}(emptyFile)
	}

	return err
}
