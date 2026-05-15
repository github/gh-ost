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
func (fbc *FileBinlogCoordinates) DisplayString() string {
	return fmt.Sprintf("%s:%d", fbc.LogFile, fbc.LogPos)
}

// String returns a user-friendly string representation of these coordinates
func (fbc FileBinlogCoordinates) String() string {
	return fbc.DisplayString()
}

// Equals tests equality of this coordinate and another one.
func (fbc *FileBinlogCoordinates) Equals(other BinlogCoordinates) bool {
	coord, ok := other.(*FileBinlogCoordinates)
	if !ok || other == nil {
		return false
	}
	return fbc.LogFile == coord.LogFile && fbc.LogPos == coord.LogPos
}

// IsEmpty returns true if the log file is empty, unnamed
func (fbc *FileBinlogCoordinates) IsEmpty() bool {
	return fbc.LogFile == ""
}

// SmallerThan returns true if this coordinate is strictly smaller than the other.
func (fbc *FileBinlogCoordinates) SmallerThan(other BinlogCoordinates) bool {
	coord, ok := other.(*FileBinlogCoordinates)
	if !ok || other == nil {
		return false
	}

	fileNumberDist := fbc.FileNumberDistance(coord)
	if fileNumberDist == 0 {
		return fbc.LogPos < coord.LogPos
	}
	return fileNumberDist > 0
}

// SmallerThanOrEquals returns true if this coordinate is the same or equal to the other one.
// We do NOT compare the type so we can not use fbc.Equals()
func (fbc *FileBinlogCoordinates) SmallerThanOrEquals(other BinlogCoordinates) bool {
	coord, ok := other.(*FileBinlogCoordinates)
	if !ok || other == nil {
		return false
	}
	if fbc.SmallerThan(other) {
		return true
	}
	return fbc.LogFile == coord.LogFile && fbc.LogPos == coord.LogPos // No Type comparison
}

// FileNumberDistance returns the numeric distance between this coordinate's file number and the other's.
// Effectively it means "how many rotates/FLUSHes would make these coordinates's file reach the other's"
func (fbc *FileBinlogCoordinates) FileNumberDistance(other *FileBinlogCoordinates) int {
	fbcNumber, _ := fbc.FileNumber()
	otherNumber, _ := other.FileNumber()
	return otherNumber - fbcNumber
}

// FileNumber returns the numeric value of the file, and the length in characters representing the number in the filename.
// Example: FileNumber() of mysqld.log.000789 is (789, 6)
func (fbc *FileBinlogCoordinates) FileNumber() (int, int) {
	tokens := strings.Split(fbc.LogFile, ".")
	numPart := tokens[len(tokens)-1]
	numLen := len(numPart)
	fileNum, err := strconv.Atoi(numPart)
	if err != nil {
		return 0, 0
	}
	return fileNum, numLen
}

// PreviousFileCoordinatesBy guesses the filename of the previous binlog/relaylog, by given offset (number of files back)
func (fbc *FileBinlogCoordinates) PreviousFileCoordinatesBy(offset int) (BinlogCoordinates, error) {
	result := &FileBinlogCoordinates{}

	fileNum, numLen := fbc.FileNumber()
	if fileNum == 0 {
		return result, errors.New("log file number is zero, cannot detect previous file")
	}
	newNumStr := fmt.Sprintf("%d", (fileNum - offset))
	newNumStr = strings.Repeat("0", numLen-len(newNumStr)) + newNumStr

	tokens := strings.Split(fbc.LogFile, ".")
	tokens[len(tokens)-1] = newNumStr
	result.LogFile = strings.Join(tokens, ".")
	return result, nil
}

// PreviousFileCoordinates guesses the filename of the previous binlog/relaylog
func (fbc *FileBinlogCoordinates) PreviousFileCoordinates() (BinlogCoordinates, error) {
	return fbc.PreviousFileCoordinatesBy(1)
}

// PreviousFileCoordinates guesses the filename of the previous binlog/relaylog
func (fbc *FileBinlogCoordinates) NextFileCoordinates() (BinlogCoordinates, error) {
	result := &FileBinlogCoordinates{}

	fileNum, numLen := fbc.FileNumber()
	newNumStr := fmt.Sprintf("%d", (fileNum + 1))
	newNumStr = strings.Repeat("0", numLen-len(newNumStr)) + newNumStr

	tokens := strings.Split(fbc.LogFile, ".")
	tokens[len(tokens)-1] = newNumStr
	result.LogFile = strings.Join(tokens, ".")
	return result, nil
}

// FileSmallerThan returns true if this coordinate's file is strictly smaller than the other's.
func (fbc *FileBinlogCoordinates) DetachedCoordinates() (isDetached bool, detachedLogFile string, detachedLogPos string) {
	detachedCoordinatesSubmatch := detachPattern.FindStringSubmatch(fbc.LogFile)
	if len(detachedCoordinatesSubmatch) == 0 {
		return false, "", ""
	}
	return true, detachedCoordinatesSubmatch[1], detachedCoordinatesSubmatch[2]
}

func (fbc *FileBinlogCoordinates) Clone() BinlogCoordinates {
	return &FileBinlogCoordinates{
		LogPos:    fbc.LogPos,
		LogFile:   fbc.LogFile,
		EventSize: fbc.EventSize,
	}
}

// IsLogPosOverflowBeyond4Bytes returns true if the coordinate endpos is overflow beyond 4 bytes.
// The binlog event end_log_pos field type is defined as uint32, 4 bytes.
// https://github.com/go-mysql-org/go-mysql/blob/master/replication/event.go
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_replication_binlog_event.html#sect_protocol_replication_binlog_event_header
// Issue: https://github.com/github/gh-ost/issues/1366
func (fbc *FileBinlogCoordinates) IsLogPosOverflowBeyond4Bytes(preCoordinate *FileBinlogCoordinates) bool {
	if preCoordinate == nil {
		return false
	}
	if preCoordinate.IsEmpty() {
		return false
	}

	if fbc.LogFile != preCoordinate.LogFile {
		return false
	}

	if preCoordinate.LogPos+fbc.EventSize >= 1<<32 {
		// Unexpected rows event, the previous binlog log_pos + current binlog event_size is overflow 4 bytes
		return true
	}
	return false
}
