package bi5

import (
	"fmt"
	"path/filepath"
	"time"
)

func BiFilePathTime(folder, symbol string, t time.Time) string {
	t = t.UTC()
	year, month, day := t.Date()
	return BiFilePath(folder, symbol, year, int(month), day, t.Hour())
}

func BiFilePath(folder string, symbol string, y, m, day, hour int) string {
	return filepath.FromSlash(fmt.Sprintf("%s/download/%s/%04d/%02d/%02d/%02dh_ticks.%s", folder, symbol, y, m, day, hour, ext))
}
