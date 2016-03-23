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

func parseEntries(entriesBytes []byte) (entries [](*BinlogEntry), err error) {
	scanner := bufio.NewScanner(bytes.NewReader(entriesBytes))
	expectEndLogPos := false
	var startLogPos uint64
	var endLogPos uint64

	binlogEntry := &BinlogEntry{}

	for scanner.Scan() {
		line := scanner.Text()

		onStartEntry := func(submatch []string) error {
			startLogPos, _ = strconv.ParseUint(submatch[1], 10, 64)

			if endLogPos != 0 && startLogPos != endLogPos {
				return fmt.Errorf("Expected startLogPos %+v to equal previous endLogPos %+v", startLogPos, endLogPos)
			}
			// We are entering a new entry, let's push the previous one
			if binlogEntry.LogPos != 0 && binlogEntry.StatementType != "" {
				entries = append(entries, binlogEntry)
				log.Debugf("entry: %+v", *binlogEntry)
				binlogEntry = &BinlogEntry{}
			}

			//log.Debugf(line)
			binlogEntry.LogPos = startLogPos
			// Next iteration we will read the end_log_pos
			expectEndLogPos = true

			return nil
		}
		if expectEndLogPos {
			submatch := endLogPosRegexp.FindStringSubmatch(line)
			if len(submatch) <= 1 {
				return entries, log.Errorf("Expected to find end_log_pos following pos %+v", startLogPos)
			}
			endLogPos, _ = strconv.ParseUint(submatch[1], 10, 64)

			binlogEntry.EndLogPos = endLogPos
			expectEndLogPos = false
		} else if submatch := startEntryRegexp.FindStringSubmatch(line); len(submatch) > 1 {
			if err := onStartEntry(submatch); err != nil {
				return entries, log.Errore(err)
			}
		} else if submatch := startEntryUnknownTableRegexp.FindStringSubmatch(line); len(submatch) > 1 {
			if err := onStartEntry(submatch); err != nil {
				return entries, log.Errore(err)
			}
		} else if submatch := statementRegxp.FindStringSubmatch(line); len(submatch) > 1 {
			binlogEntry.StatementType = strings.Split(submatch[1], " ")[0]
			binlogEntry.DatabaseName = submatch[2]
			binlogEntry.TableName = submatch[3]
		}

	}
	if binlogEntry.LogPos != 0 {
		entries = append(entries, binlogEntry)
		log.Debugf("entry: %+v", *binlogEntry)
	}
	return entries, err
}
