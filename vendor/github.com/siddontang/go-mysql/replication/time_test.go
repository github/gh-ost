package replication

import (
	"time"

	. "github.com/pingcap/check"
)

type testTimeSuite struct{}

var _ = Suite(&testTimeSuite{})

func (s *testSyncerSuite) TestTime(c *C) {
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
		t1 := fracTime{time.Date(t.year, time.Month(t.month), t.day, t.hour, t.min, t.sec, t.microSec*1000, time.UTC), t.frac}
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
