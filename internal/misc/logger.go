package misc

import (
	"fmt"
	"strings"

	log "unknwon.dev/clog/v2"
)

// Logger interface
type Logger interface {
	Trace(format string, v ...interface{})
	Info(format string, v ...interface{})
	Warn(format string, v ...interface{})
	Error(format string, v ...interface{})
	Fatal(format string, v ...interface{})
}

// NewLogger create Logger instance with `prefix`, `skip` is used for `Error` and `Fatal` stack trace.
//   Example:
//		log := NewLogger("App", 2)
//
func NewLogger(prefix string, skip int) Logger {
	return &logPrefix{
		prefix: strings.ToTitle(prefix),
		skip:   skip,
	}
}

type logPrefix struct {
	prefix string
	skip   int
}

func (l *logPrefix) Trace(format string, v ...interface{}) {
	log.Trace(l.format(format), v...)
}

func (l *logPrefix) format(format string) string {
	if l.prefix == "" {
		return format
	}
	return fmt.Sprintf("[%s] %s", l.prefix, format)
}

func (l *logPrefix) Info(format string, v ...interface{}) {
	log.Info(l.format(format), v...)
}

func (l *logPrefix) Warn(format string, v ...interface{}) {
	log.Warn(l.format(format), v...)
}

func (l *logPrefix) Error(format string, v ...interface{}) {
	log.ErrorDepth(l.skip, l.format(format), v...)
}

func (l *logPrefix) Fatal(format string, v ...interface{}) {
	log.FatalDepth(l.skip, l.format(format), v...)
}
