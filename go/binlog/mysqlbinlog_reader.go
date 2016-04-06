/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package binlog

import (
	"bufio"
	"bytes"
	"fmt"
	"path"
	"regexp"
	"strconv"
	//	"strings"

	"github.com/github/gh-osc/go/os"
	"github.com/outbrain/golib/log"
)

var (
	binlogChunkSizeBytes         uint64 = 32 * 1024 * 1024
	startEntryRegexp                    = regexp.MustCompile("^# at ([0-9]+)$")
	startEntryUnknownTableRegexp        = regexp.MustCompile("^### Row event for unknown table .*? at ([0-9]+)$")
	endLogPosRegexp                     = regexp.MustCompile("^#[0-9]{6} .*? end_log_pos ([0-9]+)")
	statementRegxp                      = regexp.MustCompile("### (INSERT INTO|UPDATE|DELETE FROM) `(.*?)`[.]`(.*?)`")
	tokenRegxp                          = regexp.MustCompile("### (WHERE|SET)$")
	positionalColumnRegexp              = regexp.MustCompile("###   @([0-9]+)=(.+)$")
)

// BinlogEntryState is a state in the binlog parser automaton / state machine
type BinlogEntryState string

// States of the state machine
const (
	InvalidState                      BinlogEntryState = "InvalidState"
	SearchForStartPosOrStatementState                  = "SearchForStartPosOrStatementState"
	ExpectEndLogPosState                               = "ExpectEndLogPosState"
	ExpectTokenState                                   = "ExpectTokenState"
	PositionalColumnAssignmentState                    = "PositionalColumnAssignmentState"
)

// MySQLBinlogReader reads binary log entries by executing the `mysqlbinlog`
// process and textually parsing its output
type MySQLBinlogReader struct {
	Basedir           string
	Datadir           string
	MySQLBinlogBinary string
}

// NewMySQLBinlogReader creates a new reader that directly parses binlog files from the filesystem
func NewMySQLBinlogReader(basedir string, datadir string) (mySQLBinlogReader *MySQLBinlogReader) {
	mySQLBinlogReader = &MySQLBinlogReader{
		Basedir: basedir,
		Datadir: datadir,
	}
	mySQLBinlogReader.MySQLBinlogBinary = path.Join(mySQLBinlogReader.Basedir, "bin/mysqlbinlog")
	return mySQLBinlogReader
}

// ReadEntries will read binlog entries from parsed text output of `mysqlbinlog` utility
func (this *MySQLBinlogReader) ReadEntries(logFile string, startPos uint64, stopPos uint64) (entries [](*BinlogEntry), err error) {
	if startPos == 0 {
		startPos = 4
	}
	done := false
	chunkStartPos := startPos
	for !done {
		chunkStopPos := chunkStartPos + binlogChunkSizeBytes
		if chunkStopPos > stopPos && stopPos != 0 {
			chunkStopPos = stopPos
		}
		log.Debugf("Next chunk range %d - %d", chunkStartPos, chunkStopPos)
		binlogFilePath := path.Join(this.Datadir, logFile)
		command := fmt.Sprintf(`%s --verbose --base64-output=DECODE-ROWS --start-position=%d --stop-position=%d %s`, this.MySQLBinlogBinary, chunkStartPos, chunkStopPos, binlogFilePath)
		entriesBytes, err := os.RunCommandWithOutput(command)
		if err != nil {
			return entries, log.Errore(err)
		}

		chunkEntries, err := parseEntries(bufio.NewScanner(bytes.NewReader(entriesBytes)), logFile)
		if err != nil {
			return entries, log.Errore(err)
		}

		if len(chunkEntries) == 0 {
			done = true
		} else {
			entries = append(entries, chunkEntries...)
			lastChunkEntry := chunkEntries[len(chunkEntries)-1]
			chunkStartPos = lastChunkEntry.EndLogPos
		}
	}
	return entries, err
}

// automaton step: accept wither beginning of new entry, or beginning of new statement
func searchForStartPosOrStatement(scanner *bufio.Scanner, binlogEntry *BinlogEntry, previousEndLogPos uint64) (nextState BinlogEntryState, nextBinlogEntry *BinlogEntry, err error) {
	onStartEntry := func(submatch []string) (BinlogEntryState, *BinlogEntry, error) {
		startLogPos, _ := strconv.ParseUint(submatch[1], 10, 64)

		if previousEndLogPos != 0 && startLogPos != previousEndLogPos {
			return InvalidState, binlogEntry, fmt.Errorf("Expected startLogPos %+v to equal previous endLogPos %+v", startLogPos, previousEndLogPos)
		}
		nextBinlogEntry = binlogEntry
		if binlogEntry.Coordinates.LogPos != 0 && binlogEntry.dmlEvent != nil {
			// Current entry is already a true entry, with startpos and with statement
			nextBinlogEntry = NewBinlogEntry(binlogEntry.Coordinates.LogFile, startLogPos)
		}
		return ExpectEndLogPosState, nextBinlogEntry, nil
	}

	onStatementEntry := func(submatch []string) (BinlogEntryState, *BinlogEntry, error) {
		nextBinlogEntry = binlogEntry
		if binlogEntry.Coordinates.LogPos != 0 && binlogEntry.dmlEvent != nil {
			// Current entry is already a true entry, with startpos and with statement
			nextBinlogEntry = binlogEntry.Duplicate()
		}
		nextBinlogEntry.dmlEvent = NewBinlogDMLEvent(submatch[2], submatch[3], ToEventDML(submatch[1]))

		return ExpectTokenState, nextBinlogEntry, nil
	}

	// Defuncting the following:

	// onPositionalColumn := func(submatch []string) (BinlogEntryState, *BinlogEntry, error) {
	// 	columnIndex, _ := strconv.ParseUint(submatch[1], 10, 64)
	// 	if _, found := binlogEntry.PositionalColumns[columnIndex]; found {
	// 		return InvalidState, binlogEntry, fmt.Errorf("Positional column %+v found more than once in %+v, statement=%+v", columnIndex, binlogEntry.LogPos, binlogEntry.dmlEvent.DML)
	// 	}
	// 	columnValue := submatch[2]
	// 	columnValue = strings.TrimPrefix(columnValue, "'")
	// 	columnValue = strings.TrimSuffix(columnValue, "'")
	// 	binlogEntry.PositionalColumns[columnIndex] = columnValue
	//
	// 	return SearchForStartPosOrStatementState, binlogEntry, nil
	// }

	line := scanner.Text()
	if submatch := startEntryRegexp.FindStringSubmatch(line); len(submatch) > 1 {
		return onStartEntry(submatch)
	}
	if submatch := startEntryUnknownTableRegexp.FindStringSubmatch(line); len(submatch) > 1 {
		return onStartEntry(submatch)
	}
	if submatch := statementRegxp.FindStringSubmatch(line); len(submatch) > 1 {
		return onStatementEntry(submatch)
	}
	if submatch := positionalColumnRegexp.FindStringSubmatch(line); len(submatch) > 1 {
		// Defuncting		return onPositionalColumn(submatch)
	}
	// Haven't found a match
	return SearchForStartPosOrStatementState, binlogEntry, nil
}

// automaton step: expect an end_log_pos line`
func expectEndLogPos(scanner *bufio.Scanner, binlogEntry *BinlogEntry) (nextState BinlogEntryState, err error) {
	line := scanner.Text()

	submatch := endLogPosRegexp.FindStringSubmatch(line)
	if len(submatch) > 1 {
		binlogEntry.EndLogPos, _ = strconv.ParseUint(submatch[1], 10, 64)
		return SearchForStartPosOrStatementState, nil
	}
	return InvalidState, fmt.Errorf("Expected to find end_log_pos following pos %+v", binlogEntry.Coordinates.LogPos)
}

// automaton step: a not-strictly-required but good-to-have-around validation that
// we see an expected token following a statement
func expectToken(scanner *bufio.Scanner, binlogEntry *BinlogEntry) (nextState BinlogEntryState, err error) {
	line := scanner.Text()
	if submatch := tokenRegxp.FindStringSubmatch(line); len(submatch) > 1 {
		return SearchForStartPosOrStatementState, nil
	}
	return InvalidState, fmt.Errorf("Expected to find token following pos %+v", binlogEntry.Coordinates.LogPos)
}

// parseEntries will parse output of `mysqlbinlog --verbose --base64-output=DECODE-ROWS`
// It issues an automaton / state machine to do its thang.
func parseEntries(scanner *bufio.Scanner, logFile string) (entries [](*BinlogEntry), err error) {
	binlogEntry := NewBinlogEntry(logFile, 0)
	var state BinlogEntryState = SearchForStartPosOrStatementState
	var endLogPos uint64

	appendBinlogEntry := func() {
		if binlogEntry.Coordinates.LogPos == 0 {
			return
		}
		if binlogEntry.dmlEvent == nil {
			return
		}
		entries = append(entries, binlogEntry)
		log.Debugf("entry: %+v", *binlogEntry)
		fmt.Println(fmt.Sprintf("%s `%s`.`%s`", binlogEntry.dmlEvent.DML, binlogEntry.dmlEvent.DatabaseName, binlogEntry.dmlEvent.TableName))
	}
	for scanner.Scan() {
		switch state {
		case SearchForStartPosOrStatementState:
			{
				var nextBinlogEntry *BinlogEntry
				state, nextBinlogEntry, err = searchForStartPosOrStatement(scanner, binlogEntry, endLogPos)
				if nextBinlogEntry != binlogEntry {
					appendBinlogEntry()
					binlogEntry = nextBinlogEntry
				}
			}
		case ExpectEndLogPosState:
			{
				state, err = expectEndLogPos(scanner, binlogEntry)
			}
		case ExpectTokenState:
			{
				state, err = expectToken(scanner, binlogEntry)
			}
		default:
			{
				err = fmt.Errorf("Unexpected state %+v", state)
			}
		}
		if err != nil {
			return entries, log.Errore(err)
		}
	}
	appendBinlogEntry()
	return entries, err
}
