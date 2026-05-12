/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package sql

import (
	"regexp"
	"strconv"
	"strings"
)

var (
	sanitizeQuotesRegexp                 = regexp.MustCompile("('[^']*')")
	renameColumnRegexp                   = regexp.MustCompile(`(?i)\bchange\s+(column\s+|)([\S]+)\s+([\S]+)\s+`)
	dropColumnRegexp                     = regexp.MustCompile(`(?i)\bdrop\s+(column\s+|)([\S]+)$`)
	renameTableRegexp                    = regexp.MustCompile(`(?i)\brename\s+(to|as)\s+`)
	autoIncrementRegexp                  = regexp.MustCompile(`(?i)\bauto_increment[\s]*=[\s]*([0-9]+)`)
	alterTableExplicitSchemaTableRegexps = []*regexp.Regexp{
		// ALTER TABLE `scm`.`tbl` something
		regexp.MustCompile(`(?i)\balter\s+table\s+` + "`" + `([^` + "`" + `]+)` + "`" + `[.]` + "`" + `([^` + "`" + `]+)` + "`" + `\s+(.*$)`),
		// ALTER TABLE `scm`.tbl something
		regexp.MustCompile(`(?i)\balter\s+table\s+` + "`" + `([^` + "`" + `]+)` + "`" + `[.]([\S]+)\s+(.*$)`),
		// ALTER TABLE scm.`tbl` something
		regexp.MustCompile(`(?i)\balter\s+table\s+([\S]+)[.]` + "`" + `([^` + "`" + `]+)` + "`" + `\s+(.*$)`),
		// ALTER TABLE scm.tbl something
		regexp.MustCompile(`(?i)\balter\s+table\s+([\S]+)[.]([\S]+)\s+(.*$)`),
	}
	alterTableExplicitTableRegexps = []*regexp.Regexp{
		// ALTER TABLE `tbl` something
		regexp.MustCompile(`(?i)\balter\s+table\s+` + "`" + `([^` + "`" + `]+)` + "`" + `\s+(.*$)`),
		// ALTER TABLE tbl something
		regexp.MustCompile(`(?i)\balter\s+table\s+([\S]+)\s+(.*$)`),
	}
	enumValuesRegexp = regexp.MustCompile("^enum[(](.*)[)]$")
)

type AlterTableParser struct {
	columnRenameMap        map[string]string
	droppedColumns         map[string]bool
	isRenameTable          bool
	isAutoIncrementDefined bool

	alterStatementOptions string
	alterTokens           []string

	explicitSchema string
	explicitTable  string
}

func NewAlterTableParser() *AlterTableParser {
	return &AlterTableParser{
		columnRenameMap: make(map[string]string),
		droppedColumns:  make(map[string]bool),
	}
}

func NewParserFromAlterStatement(alterStatement string) *AlterTableParser {
	parser := NewAlterTableParser()
	parser.ParseAlterStatement(alterStatement)
	return parser
}

func (atp *AlterTableParser) tokenizeAlterStatement(alterStatement string) (tokens []string) {
	terminatingQuote := rune(0)
	f := func(c rune) bool {
		switch {
		case c == terminatingQuote:
			terminatingQuote = rune(0)
			return false
		case terminatingQuote != rune(0):
			return false
		case c == '\'':
			terminatingQuote = c
			return false
		case c == '(':
			terminatingQuote = ')'
			return false
		default:
			return c == ','
		}
	}

	tokens = strings.FieldsFunc(alterStatement, f)
	for i := range tokens {
		tokens[i] = strings.TrimSpace(tokens[i])
	}
	return tokens
}

func (atp *AlterTableParser) sanitizeQuotesFromAlterStatement(alterStatement string) (strippedStatement string) {
	strippedStatement = alterStatement
	strippedStatement = sanitizeQuotesRegexp.ReplaceAllString(strippedStatement, "''")
	return strippedStatement
}

func (atp *AlterTableParser) parseAlterToken(alterToken string) {
	{
		// rename
		allStringSubmatch := renameColumnRegexp.FindAllStringSubmatch(alterToken, -1)
		for _, submatch := range allStringSubmatch {
			if unquoted, err := strconv.Unquote(submatch[2]); err == nil {
				submatch[2] = unquoted
			}
			if unquoted, err := strconv.Unquote(submatch[3]); err == nil {
				submatch[3] = unquoted
			}
			atp.columnRenameMap[submatch[2]] = submatch[3]
		}
	}
	{
		// drop
		allStringSubmatch := dropColumnRegexp.FindAllStringSubmatch(alterToken, -1)
		for _, submatch := range allStringSubmatch {
			if unquoted, err := strconv.Unquote(submatch[2]); err == nil {
				submatch[2] = unquoted
			}
			atp.droppedColumns[submatch[2]] = true
		}
	}
	{
		// rename table
		if renameTableRegexp.MatchString(alterToken) {
			atp.isRenameTable = true
		}
	}
	{
		// auto_increment
		if autoIncrementRegexp.MatchString(alterToken) {
			atp.isAutoIncrementDefined = true
		}
	}
}

func (atp *AlterTableParser) ParseAlterStatement(alterStatement string) (err error) {
	atp.alterStatementOptions = alterStatement
	for _, alterTableRegexp := range alterTableExplicitSchemaTableRegexps {
		if submatch := alterTableRegexp.FindStringSubmatch(atp.alterStatementOptions); len(submatch) > 0 {
			atp.explicitSchema = submatch[1]
			atp.explicitTable = submatch[2]
			atp.alterStatementOptions = submatch[3]
			break
		}
	}
	for _, alterTableRegexp := range alterTableExplicitTableRegexps {
		if submatch := alterTableRegexp.FindStringSubmatch(atp.alterStatementOptions); len(submatch) > 0 {
			atp.explicitTable = submatch[1]
			atp.alterStatementOptions = submatch[2]
			break
		}
	}
	for _, alterToken := range atp.tokenizeAlterStatement(atp.alterStatementOptions) {
		alterToken = atp.sanitizeQuotesFromAlterStatement(alterToken)
		atp.parseAlterToken(alterToken)
		atp.alterTokens = append(atp.alterTokens, alterToken)
	}
	return nil
}

func (atp *AlterTableParser) GetNonTrivialRenames() map[string]string {
	result := make(map[string]string)
	for column, renamed := range atp.columnRenameMap {
		if column != renamed {
			result[column] = renamed
		}
	}
	return result
}

func (atp *AlterTableParser) HasNonTrivialRenames() bool {
	return len(atp.GetNonTrivialRenames()) > 0
}

func (atp *AlterTableParser) DroppedColumnsMap() map[string]bool {
	return atp.droppedColumns
}

func (atp *AlterTableParser) IsRenameTable() bool {
	return atp.isRenameTable
}

func (atp *AlterTableParser) IsAutoIncrementDefined() bool {
	return atp.isAutoIncrementDefined
}

func (atp *AlterTableParser) GetExplicitSchema() string {
	return atp.explicitSchema
}

func (atp *AlterTableParser) HasExplicitSchema() bool {
	return atp.GetExplicitSchema() != ""
}

func (atp *AlterTableParser) GetExplicitTable() string {
	return atp.explicitTable
}

func (atp *AlterTableParser) HasExplicitTable() bool {
	return atp.GetExplicitTable() != ""
}

func (atp *AlterTableParser) GetAlterStatementOptions() string {
	return atp.alterStatementOptions
}

func ParseEnumValues(enumColumnType string) string {
	if submatch := enumValuesRegexp.FindStringSubmatch(enumColumnType); len(submatch) > 0 {
		return submatch[1]
	}
	return enumColumnType
}
