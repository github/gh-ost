package replication

import (
	"time"

	. "github.com/pingcap/check"
)

type testTimeSuite struct{}

var _ = Suite(&testTimeSuite{})

func (s *testTimeSuite) TestTime(c *C) {
	tbls := []struct {
		year     int
		month    int
		day      int
		hour     int
		min      int
		sec      int
		microSec int
		frac     int
		expected string
	}{
		{2000, 1, 1, 1, 1, 1, 1, 0, "2000-01-01 01:01:01"},
		{2000, 1, 1, 1, 1, 1, 1, 1, "2000-01-01 01:01:01.0"},
		{2000, 1, 1, 1, 1, 1, 1, 6, "2000-01-01 01:01:01.000001"},
	}

	for _, t := range tbls {
		t1 := fracTime{time.Date(t.year, time.Month(t.month), t.day, t.hour, t.min, t.sec, t.microSec*1000, time.UTC), t.frac, nil}
		c.Assert(t1.String(), Equals, t.expected)
	}

	zeroTbls := []struct {
		frac     int
		dec      int
		expected string
	}{
		{0, 1, "0000-00-00 00:00:00.0"},
		{1, 1, "0000-00-00 00:00:00.0"},
		{123, 3, "0000-00-00 00:00:00.000"},
		{123000, 3, "0000-00-00 00:00:00.123"},
		{123, 6, "0000-00-00 00:00:00.000123"},
		{123000, 6, "0000-00-00 00:00:00.123000"},
	}

	for _, t := range zeroTbls {
		c.Assert(formatZeroTime(t.frac, t.dec), Equals, t.expected)
	}
}

func (s *testTimeSuite) TestTimeStringLocation(c *C) {
	t := fracTime{
		time.Date(2018, time.Month(7), 30, 10, 0, 0, 0, time.FixedZone("EST", -5*3600)),
		0,
		nil,
	}

	c.Assert(t.String(), Equals, "2018-07-30 10:00:00")

	t = fracTime{
		time.Date(2018, time.Month(7), 30, 10, 0, 0, 0, time.FixedZone("EST", -5*3600)),
		0,
		time.UTC,
	}
	c.Assert(t.String(), Equals, "2018-07-30 15:00:00")
}

var _ = Suite(&testTimeSuite{})
