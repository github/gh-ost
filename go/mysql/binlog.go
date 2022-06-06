/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	"fmt"
	"strconv"
	"strings"
)

// BinlogCoordinates described binary log coordinates in the form of log file & log position.
type BinlogCoordinates struct {
	LogFile string
	LogPos  int64
}

// ParseInstanceKey will parse an InstanceKey from a string representation such as 127.0.0.1:3306
func ParseBinlogCoordinates(logFileLogPos string) (*BinlogCoordinates, error) {
	tokens := strings.SplitN(logFileLogPos, ":", 2)
	if len(tokens) != 2 {
		return nil, fmt.Errorf("ParseBinlogCoordinates: Cannot parse BinlogCoordinates from %s. Expected format is file:pos", logFileLogPos)
	}

	if logPos, err := strconv.ParseInt(tokens[1], 10, 0); err != nil {
		return nil, fmt.Errorf("ParseBinlogCoordinates: invalid pos: %s", tokens[1])
	} else {
		return &BinlogCoordinates{LogFile: tokens[0], LogPos: logPos}, nil
	}
}

// DisplayString returns a user-friendly string representation of these coordinates
func (this *BinlogCoordinates) DisplayString() string {
	return fmt.Sprintf("%s:%d", this.LogFile, this.LogPos)
}

// String returns a user-friendly string representation of these coordinates
func (this BinlogCoordinates) String() string {
	return this.DisplayString()
}

// Equals tests equality of this coordinate and another one.
func (this *BinlogCoordinates) Equals(other *BinlogCoordinates) bool {
	if other == nil {
		return false
	}
	return this.LogFile == other.LogFile && this.LogPos == other.LogPos
}

// IsEmpty returns true if the log file is empty, unnamed
func (this *BinlogCoordinates) IsEmpty() bool {
	return this.LogFile == ""
}

// SmallerThan returns true if this coordinate is strictly smaller than the other.
func (this *BinlogCoordinates) SmallerThan(other *BinlogCoordinates) bool {
	if this.LogFile < other.LogFile {
		return true
	}
	if this.LogFile == other.LogFile && this.LogPos < other.LogPos {
		return true
	}
	return false
}

// SmallerThanOrEquals returns true if this coordinate is the same or equal to the other one.
// We do NOT compare the type so we can not use this.Equals()
func (this *BinlogCoordinates) SmallerThanOrEquals(other *BinlogCoordinates) bool {
	if this.SmallerThan(other) {
		return true
	}
	return this.LogFile == other.LogFile && this.LogPos == other.LogPos
}
