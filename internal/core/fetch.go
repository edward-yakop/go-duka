package core

import (
	"fmt"
	"github.com/pkg/errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"ed-fx/go-duka/internal/misc"
)

const (
	// "https://datafeed.dukascopy.com/datafeed/{currency}/{year}/{month:02d}/{day:02d}/{hour:02d}h_ticks.bi5"
	DukaTmplURL = "https://datafeed.dukascopy.com/datafeed/%s/%04d/%02d/%02d/%02dh_ticks.bi5"
	retryTimes  = 5
)

var (
	log = misc.NewLogger("Duka", 2)
)

type HTTPDownload struct {
	client *http.Client
}

func NewDownloader() Downloader {
	return &HTTPDownload{
		client: &http.Client{Timeout: 5 * time.Minute},
	}
}

func (h HTTPDownload) Download(URL string, toFilePath string) (httpStatusCode int, filesize int64, err error) {
	var resp *http.Response
	for retry := 0; retry < retryTimes; retry++ {
		resp, err = h.client.Get(URL)
		if err != nil {
			log.Error("[%d] Download %s failed: %v.", retry, URL, err)
			h.delay()

			continue
		}
		defer resp.Body.Close()

		httpStatusCode = resp.StatusCode
		if httpStatusCode != http.StatusOK {
			log.Warn("[%d] Download %s failed %d:%s.", retry, URL, httpStatusCode, resp.Status)
			if httpStatusCode == http.StatusNotFound {
				// 404
				break
			}

			err = fmt.Errorf("http error %d:%s", resp.StatusCode, resp.Status)
			h.delay()
			continue
		}

		filesize, err = h.saveBodyToDisk(resp.Body, toFilePath)
		return
	}

	return
}

func (h HTTPDownload) delay() {
	time.Sleep(5 * time.Second)
}

func (h HTTPDownload) saveBodyToDisk(body io.ReadCloser, path string) (filesize int64, err error) {
	// Create dir if not exists
	dir := filepath.Dir(path)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		err = errors.Wrap(err, "Create folder ["+dir+"] failed")
		return
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	if err != nil {
		err = errors.Wrap(err, "Create file ["+path+"] failed")
		return
	}

	defer f.Close()
	filesize, err = io.Copy(f, body)
	if err != nil {
		err = errors.Wrap(err, "Saving tick data ["+path+"] failed")
		return
	}

	return
}
