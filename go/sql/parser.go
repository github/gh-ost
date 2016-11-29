/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package sql

import (
	"regexp"
	"strconv"
	"strings"
)

var (
	stripQuotesRegexp  = regexp.MustCompile("('[^']*')")
	renameColumnRegexp = regexp.MustCompile(`(?i)\bchange\s+(column\s+|)([\S]+)\s+([\S]+)\s+`)
)

type Parser struct {
	columnRenameMap map[string]string
}

func NewParser() *Parser {
	return &Parser{
		columnRenameMap: make(map[string]string),
	}
}

func (this *Parser) tokenizeAlterStatement(alterStatement string) (tokens []string, err error) {
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
	return tokens, nil
}

func (this *Parser) stripQuotesFromAlterStatement(alterStatement string) (strippedStatement string) {
	strippedStatement = alterStatement
	strippedStatement = stripQuotesRegexp.ReplaceAllString(strippedStatement, "''")
	return strippedStatement
}

func (this *Parser) ParseAlterStatement(alterStatement string) (err error) {
	alterTokens, _ := this.tokenizeAlterStatement(alterStatement)
	for _, alterToken := range alterTokens {
		alterToken = this.stripQuotesFromAlterStatement(alterToken)
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
	return nil
}

func (this *Parser) GetNonTrivialRenames() map[string]string {
	result := make(map[string]string)
	for column, renamed := range this.columnRenameMap {
		if column != renamed {
			result[column] = renamed
		}
	}
	return result
}

func (this *Parser) HasNonTrivialRenames() bool {
	return len(this.GetNonTrivialRenames()) > 0
}
