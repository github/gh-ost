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

func (this *AlterTableParser) tokenizeAlterStatement(alterStatement string) (tokens []string) {
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

func (this *AlterTableParser) sanitizeQuotesFromAlterStatement(alterStatement string) (strippedStatement string) {
	strippedStatement = alterStatement
	strippedStatement = sanitizeQuotesRegexp.ReplaceAllString(strippedStatement, "''")
	return strippedStatement
}

func (this *AlterTableParser) parseAlterToken(alterToken string) {
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
			this.columnRenameMap[submatch[2]] = submatch[3]
		}
	}
	{
		// drop
		allStringSubmatch := dropColumnRegexp.FindAllStringSubmatch(alterToken, -1)
		for _, submatch := range allStringSubmatch {
			if unquoted, err := strconv.Unquote(submatch[2]); err == nil {
				submatch[2] = unquoted
			}
			this.droppedColumns[submatch[2]] = true
		}
	}
	{
		// rename table
		if renameTableRegexp.MatchString(alterToken) {
			this.isRenameTable = true
		}
	}
	{
		// auto_increment
		if autoIncrementRegexp.MatchString(alterToken) {
			this.isAutoIncrementDefined = true
		}
	}
}

func (this *AlterTableParser) ParseAlterStatement(alterStatement string) (err error) {
	this.alterStatementOptions = alterStatement
	for _, alterTableRegexp := range alterTableExplicitSchemaTableRegexps {
		if submatch := alterTableRegexp.FindStringSubmatch(this.alterStatementOptions); len(submatch) > 0 {
			this.explicitSchema = submatch[1]
			this.explicitTable = submatch[2]
			this.alterStatementOptions = submatch[3]
			break
		}
	}
	for _, alterTableRegexp := range alterTableExplicitTableRegexps {
		if submatch := alterTableRegexp.FindStringSubmatch(this.alterStatementOptions); len(submatch) > 0 {
			this.explicitTable = submatch[1]
			this.alterStatementOptions = submatch[2]
			break
		}
	}
	for _, alterToken := range this.tokenizeAlterStatement(this.alterStatementOptions) {
		alterToken = this.sanitizeQuotesFromAlterStatement(alterToken)
		this.parseAlterToken(alterToken)
		this.alterTokens = append(this.alterTokens, alterToken)
	}
	return nil
}

func (this *AlterTableParser) GetNonTrivialRenames() map[string]string {
	result := make(map[string]string)
	for column, renamed := range this.columnRenameMap {
		if column != renamed {
			result[column] = renamed
		}
	}
	return result
}

func (this *AlterTableParser) HasNonTrivialRenames() bool {
	return len(this.GetNonTrivialRenames()) > 0
}

func (this *AlterTableParser) DroppedColumnsMap() map[string]bool {
	return this.droppedColumns
}

func (this *AlterTableParser) IsRenameTable() bool {
	return this.isRenameTable
}

func (this *AlterTableParser) IsAutoIncrementDefined() bool {
	return this.isAutoIncrementDefined
}

func (this *AlterTableParser) GetExplicitSchema() string {
	return this.explicitSchema
}

func (this *AlterTableParser) HasExplicitSchema() bool {
	return this.GetExplicitSchema() != ""
}

func (this *AlterTableParser) GetExplicitTable() string {
	return this.explicitTable
}

func (this *AlterTableParser) HasExplicitTable() bool {
	return this.GetExplicitTable() != ""
}

func (this *AlterTableParser) GetAlterStatementOptions() string {
	return this.alterStatementOptions
}

func ParseEnumValues(enumColumnType string) string {
	if submatch := enumValuesRegexp.FindStringSubmatch(enumColumnType); len(submatch) > 0 {
		return submatch[1]
	}
	return enumColumnType
}
