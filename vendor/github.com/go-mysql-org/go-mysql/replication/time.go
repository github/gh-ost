package replication

import (
	"fmt"
	"strings"
	"time"
)

var (
	fracTimeFormat []string
)

// fracTime is a help structure wrapping Golang Time.
type fracTime struct {
	time.Time

	// Dec must in [0, 6]
	Dec int

	timestampStringLocation *time.Location
}

func (t fracTime) String() string {
	tt := t.Time
	if t.timestampStringLocation != nil {
		tt = tt.In(t.timestampStringLocation)
	}
	return tt.Format(fracTimeFormat[t.Dec])
}

func formatZeroTime(frac int, dec int) string {
	if dec == 0 {
		return "0000-00-00 00:00:00"
	}

	s := fmt.Sprintf("0000-00-00 00:00:00.%06d", frac)

	// dec must < 6, if frac is 924000, but dec is 3, we must output 924 here.
	return s[0 : len(s)-(6-dec)]
}

func formatBeforeUnixZeroTime(year, month, day, hour, minute, second, frac, dec int) string {
	if dec == 0 {
		return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second)
	}

	s := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hour, minute, second, frac)

	// dec must < 6, if frac is 924000, but dec is 3, we must output 924 here.
	return s[0 : len(s)-(6-dec)]
}

func microSecTimestampToTime(ts uint64) time.Time {
	if ts == 0 {
		return time.Time{}
	}
	return time.Unix(int64(ts/1000000), int64(ts%1000000)*1000)
}

func init() {
	fracTimeFormat = make([]string, 7)
	fracTimeFormat[0] = "2006-01-02 15:04:05"

	for i := 1; i <= 6; i++ {
		fracTimeFormat[i] = fmt.Sprintf("2006-01-02 15:04:05.%s", strings.Repeat("0", i))
	}
}
