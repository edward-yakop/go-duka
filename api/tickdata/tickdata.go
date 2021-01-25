package tickdata

import (
	"fmt"
	"time"
)

// TickData for dukascopy
//
type TickData struct {
	Symbol    string  // 货币对
	Timestamp int64   // 时间戳(ms)
	Ask       float64 // 卖价
	Bid       float64 // 买价
	VolumeAsk float64 // 单位：通常是按10万美元为一手，最小0.01手
	VolumeBid float64 // 单位：...

	timestampInUTC time.Time
}

// UTC convert timestamp to UTC time
//
const timeMillisecond = int64(time.Millisecond)

func (t *TickData) UTC() time.Time {
	if t.timestampInUTC.IsZero() {
		t.timestampInUTC = time.Unix(t.Timestamp/1000, (t.Timestamp%1000)*timeMillisecond).In(time.UTC)
	}
	return t.timestampInUTC
}

func (t TickData) TimeInLocation(location *time.Location) time.Time {
	return t.UTC().In(location)
}

func (t TickData) String() string {
	return fmt.Sprintf("%s %.5f %.5f %.2f %.2f",
		t.UTC().Format("2006-01-02 15:04:06.000"),
		t.Ask,
		t.Bid,
		t.VolumeAsk,
		t.VolumeBid,
	)
}

func (t TickData) StringUnix() string {
	return fmt.Sprintf("%v %.5f %.5f %.2f %.2f",
		t.Timestamp,
		t.Ask,
		t.Bid,
		t.VolumeAsk,
		t.VolumeBid,
	)
}
