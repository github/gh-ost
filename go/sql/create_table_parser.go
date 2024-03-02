/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package sql

type CreateTableParser struct {
	createTableStatementBody string

	explicitSchema         string
	explicitTable          string
	hasForeignKeys         bool
	isAutoIncrementDefined bool
}

func NewCreateTableParser(statement string) *CreateTableParser {
	return &CreateTableParser{
		createTableStatementBody: statement,
	}
}

func NewParserFromCreateTableStatement(alterStatement string) *CreateTableParser {
	parser := NewCreateTableParser(alterStatement)
	parser.ParseStatement()
	return parser
}

func (this *CreateTableParser) ParseStatement() (err error) {
	for _, createTableRegexp := range createTableExplicitSchemaTableRegexps {
		if submatch := createTableRegexp.FindStringSubmatch(this.createTableStatementBody); len(submatch) > 0 {
			this.explicitSchema = submatch[1]
			this.explicitTable = submatch[2]
			this.createTableStatementBody = submatch[3]
			break
		}
	}
	for _, createTableRegexp := range createTableExplicitTableRegexps {
		if submatch := createTableRegexp.FindStringSubmatch(this.createTableStatementBody); len(submatch) > 0 {
			this.explicitTable = submatch[1]
			this.createTableStatementBody = submatch[2]
			break
		}
	}
	for _, token := range tokenizeStatement(this.createTableStatementBody) {
		token = sanitizeQuotesFromToken(token)
		this.parseCreateTableToken(token)
	}
	return nil
}

func (this *CreateTableParser) parseCreateTableToken(token string) {
	{
		// foreign key.
		if foreignKeyTableRegexp.MatchString(token) {
			this.hasForeignKeys = true
		}
	}
	{
		// auto_increment
		if autoIncrementRegexp.MatchString(token) {
			this.isAutoIncrementDefined = true
		}
	}
}

func (this *CreateTableParser) Type() ParserType {
	return ParserTypeCreateTable
}

func (this *CreateTableParser) GetNonTrivialRenames() map[string]string {
	return make(map[string]string)
}

func (this *CreateTableParser) HasNonTrivialRenames() bool {
	return len(this.GetNonTrivialRenames()) > 0
}

func (this *CreateTableParser) DroppedColumnsMap() map[string]bool {
	return nil
}

func (this *CreateTableParser) IsRenameTable() bool {
	// We always return false because we need to check for table renames manually
	// outside the parser.
	return false
}

func (this *CreateTableParser) IsAutoIncrementDefined() bool {
	return this.isAutoIncrementDefined
}

func (this *CreateTableParser) GetExplicitSchema() string {
	return this.explicitSchema
}

func (this *CreateTableParser) HasExplicitSchema() bool {
	return this.GetExplicitSchema() != ""
}

func (this *CreateTableParser) GetExplicitTable() string {
	return this.explicitTable
}

func (this *CreateTableParser) HasExplicitTable() bool {
	return this.GetExplicitTable() != ""
}

func (this *CreateTableParser) GetOptions() string {
	return this.createTableStatementBody
}

func (this *CreateTableParser) HasForeignKeys() bool {
	return this.hasForeignKeys
}
