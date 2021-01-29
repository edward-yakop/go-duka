package misc

import "time"

func ToHour(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
}

func ToHourUTC(t time.Time) time.Time {
	return ToHour(t).UTC()
}

func TimeToDayString(t time.Time) string {
	return t.Format("2006-01-02")
}
