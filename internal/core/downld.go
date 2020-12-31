package core

// Downloader interface...
type Downloader interface {
	Download(URL string, toFilePath string) (httpStatusCode int, filesize int64, err error)
}
