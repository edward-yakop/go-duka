package core

import (
	"fmt"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/ed-fx/go-duka/internal/misc"
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
			if httpStatusCode == http.StatusNotFound {
				// 404
				break
			}

			log.Trace("[%d] Download %s failed %d:%s. Retrying", retry, URL, httpStatusCode, resp.Status)
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
	tempFile, err := ioutil.TempFile(os.TempDir(), "go-duka.download.*.temp")
	if err != nil {
		return 0, errors.New("Failed to create temp file for download")
	}
	tempFileName := tempFile.Name()
	defer func() {
		if err != nil {
			_ = tempFile.Close()
			_ = os.Remove(tempFileName)
		}
	}()

	filesize, err = io.Copy(tempFile, body)
	if err != nil {
		err = errors.Wrap(err, "Saving tick data ["+tempFileName+"] Failed")
		return
	}

	dir := filepath.Dir(path)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		err = errors.Wrap(err, "Create folder ["+dir+"] failed")
		return
	}

	err = tempFile.Close()
	if err != nil {
		err = errors.Wrap(err, "Failed to close tick data ["+tempFileName+"] file")
		return
	}
	err = os.Rename(tempFileName, path)
	if err != nil {
		err = errors.Wrap(err, "Failed to move tick data to ["+path+"]")
	}
	return
}
