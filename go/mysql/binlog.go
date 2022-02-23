/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

var detachPattern *regexp.Regexp

func init() {
	detachPattern, _ = regexp.Compile(`//([^/:]+):([\d]+)`) // e.g. `//binlog.01234:567890`
}

type BinlogType int

const (
	BinaryLog BinlogType = iota
	RelayLog
)

// BinlogCoordinates described binary log coordinates in the form of a GTID set and/or log file & log position.
type BinlogCoordinates struct {
	GTIDSet *gomysql.MysqlGTIDSet
	LogFile string
	LogPos  int64
	Type    BinlogType
}

// ParseFileBinlogCoordinates parses a log file/position string into a *BinlogCoordinates struct.
func ParseFileBinlogCoordinates(logFileLogPos string) (*BinlogCoordinates, error) {
	tokens := strings.SplitN(logFileLogPos, ":", 2)
	if len(tokens) != 2 {
		return nil, fmt.Errorf("ParseFileBinlogCoordinates: Cannot parse BinlogCoordinates from %s. Expected format is file:pos", logFileLogPos)
	}

	if logPos, err := strconv.ParseInt(tokens[1], 10, 0); err != nil {
		return nil, fmt.Errorf("ParseFileBinlogCoordinates: invalid pos: %s", tokens[1])
	} else {
		return &BinlogCoordinates{LogFile: tokens[0], LogPos: logPos}, nil
	}
}

// ParseGTIDSetBinlogCoordinates parses a MySQL GTID set into a *BinlogCoordinates struct.
func ParseGTIDSetBinlogCoordinates(gtidSet string) (*BinlogCoordinates, error) {
	set, err := gomysql.ParseMysqlGTIDSet(gtidSet)
	return &BinlogCoordinates{GTIDSet: set.(*gomysql.MysqlGTIDSet)}, err
}

// DisplayString returns a user-friendly string representation of these coordinates
func (this *BinlogCoordinates) DisplayString() string {
	if this.GTIDSet != nil {
		return this.GTIDSet.String()
	}
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
	if this.GTIDSet != nil {
		return other.GTIDSet != nil && this.GTIDSet.Equal(other.GTIDSet)
	}
	return this.LogFile == other.LogFile && this.LogPos == other.LogPos && this.Type == other.Type
}

// IsEmpty returns true if the log file and GTID set is empty, unnamed
func (this *BinlogCoordinates) IsEmpty() bool {
	return this.LogFile == "" && this.GTIDSet == nil
}

// SmallerThan returns true if this coordinate is strictly smaller than the other.
func (this *BinlogCoordinates) SmallerThan(other *BinlogCoordinates) bool {
	// if GTID SIDs are equal we compare the interval stop points
	// if GTID SIDs differ we have to assume there is a new/larger event
	if this.GTIDSet != nil && this.GTIDSet.Sets != nil {
		if other.GTIDSet == nil || other.GTIDSet.Sets == nil {
			return false
		}
		if len(this.GTIDSet.Sets) < len(other.GTIDSet.Sets) {
			return true
		}
		for sid, otherSet := range other.GTIDSet.Sets {
			thisSet, ok := this.GTIDSet.Sets[sid]
			if !ok {
				return true // 'this' is missing an SID
			}
			if len(thisSet.Intervals) < len(otherSet.Intervals) {
				return true // 'this' has fewer intervals
			}
			for i, otherInterval := range otherSet.Intervals {
				if len(thisSet.Intervals)-1 > i {
					return true
				}
				thisInterval := thisSet.Intervals[i]
				if thisInterval.Start < otherInterval.Start || thisInterval.Stop < otherInterval.Stop {
					return true
				}
			}
		}
	} else {
		if this.LogFile < other.LogFile {
			return true
		}
		if this.LogFile == other.LogFile && this.LogPos < other.LogPos {
			return true
		}
	}
	return false
}

// SmallerThanOrEquals returns true if this coordinate is the same or equal to the other one.
// We do NOT compare the type so we can not use this.Equals()
func (this *BinlogCoordinates) SmallerThanOrEquals(other *BinlogCoordinates) bool {
	if this.SmallerThan(other) {
		return true
	}
	if this.GTIDSet != nil {
		return other.GTIDSet != nil && this.GTIDSet.Equal(other.GTIDSet)
	}
	return this.LogFile == other.LogFile && this.LogPos == other.LogPos // No Type comparison
}

// FileSmallerThan returns true if this coordinate's file is strictly smaller than the other's.
func (this *BinlogCoordinates) FileSmallerThan(other *BinlogCoordinates) bool {
	return this.LogFile < other.LogFile
}

// FileNumberDistance returns the numeric distance between this coordinate's file number and the other's.
// Effectively it means "how many rotates/FLUSHes would make these coordinates's file reach the other's"
func (this *BinlogCoordinates) FileNumberDistance(other *BinlogCoordinates) int {
	thisNumber, _ := this.FileNumber()
	otherNumber, _ := other.FileNumber()
	return otherNumber - thisNumber
}

// FileNumber returns the numeric value of the file, and the length in characters representing the number in the filename.
// Example: FileNumber() of mysqld.log.000789 is (789, 6)
func (this *BinlogCoordinates) FileNumber() (int, int) {
	tokens := strings.Split(this.LogFile, ".")
	numPart := tokens[len(tokens)-1]
	numLen := len(numPart)
	fileNum, err := strconv.Atoi(numPart)
	if err != nil {
		return 0, 0
	}
	return fileNum, numLen
}

// PreviousFileCoordinatesBy guesses the filename of the previous binlog/relaylog, by given offset (number of files back)
func (this *BinlogCoordinates) PreviousFileCoordinatesBy(offset int) (BinlogCoordinates, error) {
	result := BinlogCoordinates{LogPos: 0, Type: this.Type}

	fileNum, numLen := this.FileNumber()
	if fileNum == 0 {
		return result, errors.New("Log file number is zero, cannot detect previous file")
	}
	newNumStr := fmt.Sprintf("%d", (fileNum - offset))
	newNumStr = strings.Repeat("0", numLen-len(newNumStr)) + newNumStr

	tokens := strings.Split(this.LogFile, ".")
	tokens[len(tokens)-1] = newNumStr
	result.LogFile = strings.Join(tokens, ".")
	return result, nil
}

// PreviousFileCoordinates guesses the filename of the previous binlog/relaylog
func (this *BinlogCoordinates) PreviousFileCoordinates() (BinlogCoordinates, error) {
	return this.PreviousFileCoordinatesBy(1)
}

// PreviousFileCoordinates guesses the filename of the previous binlog/relaylog
func (this *BinlogCoordinates) NextFileCoordinates() (BinlogCoordinates, error) {
	result := BinlogCoordinates{LogPos: 0, Type: this.Type}

	fileNum, numLen := this.FileNumber()
	newNumStr := fmt.Sprintf("%d", (fileNum + 1))
	newNumStr = strings.Repeat("0", numLen-len(newNumStr)) + newNumStr

	tokens := strings.Split(this.LogFile, ".")
	tokens[len(tokens)-1] = newNumStr
	result.LogFile = strings.Join(tokens, ".")
	return result, nil
}

// FileSmallerThan returns true if this coordinate's file is strictly smaller than the other's.
func (this *BinlogCoordinates) DetachedCoordinates() (isDetached bool, detachedLogFile string, detachedLogPos string) {
	detachedCoordinatesSubmatch := detachPattern.FindStringSubmatch(this.LogFile)
	if len(detachedCoordinatesSubmatch) == 0 {
		return false, "", ""
	}
	return true, detachedCoordinatesSubmatch[1], detachedCoordinatesSubmatch[2]
}
