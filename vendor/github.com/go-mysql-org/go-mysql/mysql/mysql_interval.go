package mysql

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
)

// Like MySQL GTID Interval struct, [start, stop), left closed and right open
// See MySQL rpl_gtid.h
type Interval struct {
	// The start of this interval.
	Start int64
	// The stop of this interval.
	Stop int64
}

// Interval is [start, stop), but the GTID string's format is [n] or [n1-n2], closed interval
func parseInterval(str string) (i Interval, err error) {
	p := strings.Split(str, "-")

	switch len(p) {
	case 1:
		i.Start, err = strconv.ParseInt(p[0], 10, 64)
		if err != nil {
			return i, errors.Errorf("invalid interval format for '%s', not numeric: %v ", p[0], err)
		}
		i.Stop = i.Start + 1
	case 2:
		i.Start, err = strconv.ParseInt(p[0], 10, 64)
		if err == nil {
			i.Stop, err = strconv.ParseInt(p[1], 10, 64)
			i.Stop++
		}
	default:
		err = errors.New("invalid interval format, must n[-n]")
	}

	if err != nil {
		return i, err
	}

	if i.Stop <= i.Start {
		err = errors.New("invalid interval format, must n[-n] and the end must >= start")
	}

	return i, err
}

func (i Interval) String() (s string) {
	if i.Stop == i.Start+1 {
		return fmt.Sprintf("%d", i.Start)
	}
	return fmt.Sprintf("%d-%d", i.Start, i.Stop-1)
}

type IntervalSlice []Interval

func (s IntervalSlice) Len() int {
	return len(s)
}

// Sort is sorting intervals.
func (s IntervalSlice) Sort() {
	slices.SortFunc(s, func(a, b Interval) int {
		if a.Start < b.Start {
			return -1
		} else if a.Start > b.Start {
			return 1
		}
		if a.Stop < b.Stop {
			return -1
		} else if a.Stop > b.Stop {
			return 1
		}
		return 0
	})
}

func (s IntervalSlice) Normalize() IntervalSlice {
	if s == nil {
		return nil
	}

	if len(s) == 0 {
		return s
	}
	n := make(IntervalSlice, 0, len(s))

	s.Sort()

	n = append(n, s[0])

	for i := 1; i < len(s); i++ {
		last := n[len(n)-1]
		if s[i].Start > last.Stop {
			n = append(n, s[i])
			continue
		}
		stop := max(last.Stop, s[i].Stop)
		n[len(n)-1] = Interval{last.Start, stop}
	}

	return n
}

// Contain returns true if sub in s. s must be sorted and normalized; sub may
// be in any order.
func (s IntervalSlice) Contain(sub IntervalSlice) bool {
	for i := range sub {
		j := sort.Search(len(s), func(j int) bool {
			return sub[i].Start <= s[j].Stop
		})
		if j == len(s) {
			return false
		}
		if sub[i].Start < s[j].Start || sub[i].Stop > s[j].Stop {
			return false
		}
	}
	return true
}

func (s IntervalSlice) Equal(o IntervalSlice) bool {
	return slices.Equal(s, o)
}
