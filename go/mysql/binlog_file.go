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
)

var detachPattern *regexp.Regexp

func init() {
	detachPattern, _ = regexp.Compile(`//([^/:]+):([\d]+)`) // e.g. `//binlog.01234:567890`
}

// FileBinlogCoordinates described binary log coordinates in the form of a binlog file & log position.
type FileBinlogCoordinates struct {
	LogFile   string
	LogPos    int64
	EventSize int64
}

func NewFileBinlogCoordinates(logFile string, logPos int64) *FileBinlogCoordinates {
	return &FileBinlogCoordinates{
		LogFile: logFile,
		LogPos:  logPos,
	}
}

// ParseFileBinlogCoordinates parses a log file/position string into a *BinlogCoordinates struct.
func ParseFileBinlogCoordinates(logFileLogPos string) (*FileBinlogCoordinates, error) {
	tokens := strings.SplitN(logFileLogPos, ":", 2)
	if len(tokens) != 2 {
		return nil, fmt.Errorf("ParseFileBinlogCoordinates: Cannot parse BinlogCoordinates from %s. Expected format is file:pos", logFileLogPos)
	}

	if logPos, err := strconv.ParseInt(tokens[1], 10, 0); err != nil {
		return nil, fmt.Errorf("ParseFileBinlogCoordinates: invalid pos: %s", tokens[1])
	} else {
		return &FileBinlogCoordinates{LogFile: tokens[0], LogPos: logPos}, nil
	}
}

// DisplayString returns a user-friendly string representation of these coordinates
func (this *FileBinlogCoordinates) DisplayString() string {
	return fmt.Sprintf("%s:%d", this.LogFile, this.LogPos)
}

// String returns a user-friendly string representation of these coordinates
func (this FileBinlogCoordinates) String() string {
	return this.DisplayString()
}

// Equals tests equality of this coordinate and another one.
func (this *FileBinlogCoordinates) Equals(other BinlogCoordinates) bool {
	coord, ok := other.(*FileBinlogCoordinates)
	if !ok || other == nil {
		return false
	}
	return this.LogFile == coord.LogFile && this.LogPos == coord.LogPos
}

// IsEmpty returns true if the log file is empty, unnamed
func (this *FileBinlogCoordinates) IsEmpty() bool {
	return this.LogFile == ""
}

// SmallerThan returns true if this coordinate is strictly smaller than the other.
func (this *FileBinlogCoordinates) SmallerThan(other BinlogCoordinates) bool {
	coord, ok := other.(*FileBinlogCoordinates)
	if !ok || other == nil {
		return false
	}
	if this.LogFile < coord.LogFile {
		return true
	}
	if this.LogFile == coord.LogFile && this.LogPos < coord.LogPos {
		return true
	}
	return false
}

// SmallerThanOrEquals returns true if this coordinate is the same or equal to the other one.
// We do NOT compare the type so we can not use this.Equals()
func (this *FileBinlogCoordinates) SmallerThanOrEquals(other BinlogCoordinates) bool {
	coord, ok := other.(*FileBinlogCoordinates)
	if !ok || other == nil {
		return false
	}
	if this.SmallerThan(other) {
		return true
	}
	return this.LogFile == coord.LogFile && this.LogPos == coord.LogPos // No Type comparison
}

// FileSmallerThan returns true if this coordinate's file is strictly smaller than the other's.
func (this *FileBinlogCoordinates) FileSmallerThan(other BinlogCoordinates) bool {
	coord, ok := other.(*FileBinlogCoordinates)
	if !ok || other == nil {
		return false
	}
	return this.LogFile < coord.LogFile
}

// FileNumberDistance returns the numeric distance between this coordinate's file number and the other's.
// Effectively it means "how many rotates/FLUSHes would make these coordinates's file reach the other's"
func (this *FileBinlogCoordinates) FileNumberDistance(other *FileBinlogCoordinates) int {
	thisNumber, _ := this.FileNumber()
	otherNumber, _ := other.FileNumber()
	return otherNumber - thisNumber
}

// FileNumber returns the numeric value of the file, and the length in characters representing the number in the filename.
// Example: FileNumber() of mysqld.log.000789 is (789, 6)
func (this *FileBinlogCoordinates) FileNumber() (int, int) {
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
func (this *FileBinlogCoordinates) PreviousFileCoordinatesBy(offset int) (BinlogCoordinates, error) {
	result := &FileBinlogCoordinates{}

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
func (this *FileBinlogCoordinates) PreviousFileCoordinates() (BinlogCoordinates, error) {
	return this.PreviousFileCoordinatesBy(1)
}

// PreviousFileCoordinates guesses the filename of the previous binlog/relaylog
func (this *FileBinlogCoordinates) NextFileCoordinates() (BinlogCoordinates, error) {
	result := &FileBinlogCoordinates{}

	fileNum, numLen := this.FileNumber()
	newNumStr := fmt.Sprintf("%d", (fileNum + 1))
	newNumStr = strings.Repeat("0", numLen-len(newNumStr)) + newNumStr

	tokens := strings.Split(this.LogFile, ".")
	tokens[len(tokens)-1] = newNumStr
	result.LogFile = strings.Join(tokens, ".")
	return result, nil
}

// FileSmallerThan returns true if this coordinate's file is strictly smaller than the other's.
func (this *FileBinlogCoordinates) DetachedCoordinates() (isDetached bool, detachedLogFile string, detachedLogPos string) {
	detachedCoordinatesSubmatch := detachPattern.FindStringSubmatch(this.LogFile)
	if len(detachedCoordinatesSubmatch) == 0 {
		return false, "", ""
	}
	return true, detachedCoordinatesSubmatch[1], detachedCoordinatesSubmatch[2]
}

func (this *FileBinlogCoordinates) Clone() BinlogCoordinates {
	return &FileBinlogCoordinates{
		LogPos:    this.LogPos,
		LogFile:   this.LogFile,
		EventSize: this.EventSize,
	}
}

// IsLogPosOverflowBeyond4Bytes returns true if the coordinate endpos is overflow beyond 4 bytes.
// The binlog event end_log_pos field type is defined as uint32, 4 bytes.
// https://github.com/go-mysql-org/go-mysql/blob/master/replication/event.go
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_replication_binlog_event.html#sect_protocol_replication_binlog_event_header
// Issue: https://github.com/github/gh-ost/issues/1366
func (this *FileBinlogCoordinates) IsLogPosOverflowBeyond4Bytes(preCoordinate *FileBinlogCoordinates) bool {
	if preCoordinate == nil {
		return false
	}
	if preCoordinate.IsEmpty() {
		return false
	}

	if this.LogFile != preCoordinate.LogFile {
		return false
	}

	if preCoordinate.LogPos+this.EventSize >= 1<<32 {
		// Unexpected rows event, the previous binlog log_pos + current binlog event_size is overflow 4 bytes
		return true
	}
	return false
}
