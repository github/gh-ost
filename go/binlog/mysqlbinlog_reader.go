/*
  Copyright 2016 GitHub Inc.
*/

package binlog

import (
	"bufio"
	"bytes"
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"

	"github.com/github/gh-osc/go/os"
	"github.com/outbrain/golib/log"
)

var (
	binlogChunkSizeBytes         uint64 = 32 * 1024 * 1024
	startEntryRegexp                    = regexp.MustCompile("^# at ([0-9]+)$")
	startEntryUnknownTableRegexp        = regexp.MustCompile("^### Row event for unknown table .*? at ([0-9]+)$")
	endLogPosRegexp                     = regexp.MustCompile("^#[0-9]{6} .*? end_log_pos ([0-9]+)")
	statementRegxp                      = regexp.MustCompile("### (INSERT INTO|UPDATE|DELETE FROM) `(.*?)`[.]`(.*?)`")
)

type BinlogEntryState string

const (
	InvalidState                    BinlogEntryState = "InvalidState"
	SearchForStartPosState                           = "SearchForStartPosState"
	ExpectEndLogPosState                             = "ExpectEndLogPosState"
	SearchForStatementState                          = "SearchForStatementState"
	ExpectTokenState                                 = "ExpectTokenState"
	PositionalColumnAssignmentState                  = "PositionalColumnAssignmentState"
)

// MySQLBinlogReader reads binary log entries by executing the `mysqlbinlog`
// process and textually parsing its output
type MySQLBinlogReader struct {
	Basedir           string
	Datadir           string
	MySQLBinlogBinary string
}

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
		chunkEntries, err := parseEntries(entriesBytes)
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

func searchForStartPos(scanner *bufio.Scanner, binlogEntry *BinlogEntry, previousEndLogPos uint64) (nextState BinlogEntryState, nextBinlogEntry *BinlogEntry, err error) {

	onStartEntry := func(submatch []string) (BinlogEntryState, *BinlogEntry, error) {
		startLogPos, _ := strconv.ParseUint(submatch[1], 10, 64)

		if previousEndLogPos != 0 && startLogPos != previousEndLogPos {
			return InvalidState, binlogEntry, fmt.Errorf("Expected startLogPos %+v to equal previous endLogPos %+v", startLogPos, previousEndLogPos)
		}
		nextBinlogEntry = binlogEntry
		if binlogEntry.LogPos != 0 && binlogEntry.StatementType != "" {
			// Current entry is already a true entry, with startpos and with statement
			nextBinlogEntry = &BinlogEntry{}
		}

		nextBinlogEntry.LogPos = startLogPos
		return ExpectEndLogPosState, nextBinlogEntry, nil
	}

	line := scanner.Text()
	if submatch := startEntryRegexp.FindStringSubmatch(line); len(submatch) > 1 {
		return onStartEntry(submatch)
	}
	if submatch := startEntryUnknownTableRegexp.FindStringSubmatch(line); len(submatch) > 1 {
		return onStartEntry(submatch)
	}
	// Haven't found a start entry
	return SearchForStartPosState, binlogEntry, nil
}

func expectEndLogPos(scanner *bufio.Scanner, binlogEntry *BinlogEntry) (nextState BinlogEntryState, err error) {
	line := scanner.Text()

	submatch := endLogPosRegexp.FindStringSubmatch(line)
	if len(submatch) <= 1 {
		return InvalidState, fmt.Errorf("Expected to find end_log_pos following pos %+v", binlogEntry.LogPos)
	}
	binlogEntry.EndLogPos, _ = strconv.ParseUint(submatch[1], 10, 64)

	return SearchForStatementState, nil
}

func searchForStatement(scanner *bufio.Scanner, binlogEntry *BinlogEntry) (nextState BinlogEntryState, err error) {
	line := scanner.Text()

	if submatch := statementRegxp.FindStringSubmatch(line); len(submatch) > 1 {
		binlogEntry.StatementType = strings.Split(submatch[1], " ")[0]
		binlogEntry.DatabaseName = submatch[2]
		binlogEntry.TableName = submatch[3]

		return SearchForStartPosState, nil
	}
	return SearchForStatementState, nil
}

func parseEntries(entriesBytes []byte) (entries [](*BinlogEntry), err error) {
	scanner := bufio.NewScanner(bytes.NewReader(entriesBytes))
	binlogEntry := &BinlogEntry{}
	var state BinlogEntryState = SearchForStartPosState
	var endLogPos uint64

	appendBinlogEntry := func() {
		entries = append(entries, binlogEntry)
		if binlogEntry.StatementType != "" {
			log.Debugf("entry: %+v", *binlogEntry)
		}
	}
	for scanner.Scan() {
		switch state {
		case SearchForStartPosState:
			{
				var nextBinlogEntry *BinlogEntry
				state, nextBinlogEntry, err = searchForStartPos(scanner, binlogEntry, endLogPos)
				if nextBinlogEntry != binlogEntry {
					appendBinlogEntry()
					binlogEntry = nextBinlogEntry
				}
			}
		case ExpectEndLogPosState:
			{
				state, err = expectEndLogPos(scanner, binlogEntry)
			}
		case SearchForStatementState:
			{
				state, err = searchForStatement(scanner, binlogEntry)
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
	if binlogEntry.LogPos != 0 {
		appendBinlogEntry()
	}
	return entries, err
}
