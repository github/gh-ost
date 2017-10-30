// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package bytesize

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/wfxiang08/cyutils/utils/errors"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
)

const (
	B = 1 << (10 * iota)
	KB
	MB
	GB
	TB
	PB
)

var (
	BytesizeRegexp = regexp.MustCompile(`(?i)^\s*(\-?[\d\.]+)\s*([KMGTP]?B|[BKMGTP]|)\s*$`)
	digitsRegexp   = regexp.MustCompile(`^\-?\d+$`)
)

var (
	ErrBadBytesize     = errors.New("invalid byte size")
	ErrBadBytesizeUnit = errors.New("invalid byte size unit")
)

func Parse(s string) (int64, error) {
	if !BytesizeRegexp.MatchString(s) {
		return 0, errors.Trace(ErrBadBytesize)
	}

	subs := BytesizeRegexp.FindStringSubmatch(s)
	if len(subs) != 3 {
		return 0, errors.Trace(ErrBadBytesize)
	}

	size := int64(0)
	switch strings.ToUpper(string(subs[2])) {
	case "B", "":
		size = 1
	case "KB", "K":
		size = KB
	case "MB", "M":
		size = MB
	case "GB", "G":
		size = GB
	case "TB", "T":
		size = TB
	case "PB", "P":
		size = PB
	default:
		return 0, errors.Trace(ErrBadBytesizeUnit)
	}

	text := subs[1]
	if digitsRegexp.MatchString(text) {
		n, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return 0, errors.Trace(ErrBadBytesize)
		}
		size *= n
	} else {
		n, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return 0, errors.Trace(ErrBadBytesize)
		}
		size = int64(float64(size) * n)
	}
	return size, nil
}

func MustParse(s string) int64 {
	v, err := Parse(s)
	if err != nil {
		log.PanicError(err, "parse bytesize failed")
	}
	return v
}
