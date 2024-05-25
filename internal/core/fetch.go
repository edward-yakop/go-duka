package core

import (
	"github.com/go-resty/resty/v2"
	"log/slog"
	"time"
)

const (
	// "https://datafeed.dukascopy.com/datafeed/{currency}/{year}/{month:02d}/{day:02d}/{hour:02d}h_ticks.bi5"
	DukaTmplURL = "https://datafeed.dukascopy.com/datafeed/%s/%04d/%02d/%02d/%02dh_ticks.bi5"
	retryTimes  = 5
)

type HTTPDownload struct {
	client *resty.Client
}

func NewDownloader() Downloader {
	return &HTTPDownload{
		client: resty.New().
			SetRetryCount(retryTimes).
			SetRetryWaitTime(5 * time.Second),
	}
}

func (h HTTPDownload) Download(URL string, toFilePath string) (httpStatusCode int, filesize int64, err error) {
	slog.Debug(
		"about to download",
		slog.String("url", URL),
		slog.String("targetFilePath", toFilePath),
	)

	resp, getErr := h.client.R().
		SetOutput(toFilePath).
		Get(URL)

	if getErr != nil {
		err = getErr
		slog.Error("Download failed",
			slog.String("url", URL),
			slog.Any("error", err),
		)

		return
	}

	httpStatusCode = resp.StatusCode()
	filesize = resp.Size()
	slog.Debug(
		"download complete",
		slog.String("url", URL),
		slog.String("targetFilePath", toFilePath),
		slog.Duration("duration", resp.Time()),
	)

	return
}
