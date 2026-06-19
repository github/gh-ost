package stmt

import "github.com/go-mysql-org/go-mysql/mysql"

type PreparedStmt struct {
	ID      uint32
	Params  int
	Columns int

	RawParamFields  [][]byte
	RawColumnFields [][]byte

	paramFields  []*mysql.Field
	columnFields []*mysql.Field
}

func (s *PreparedStmt) GetParamFields() ([]*mysql.Field, error) {
	if s.RawParamFields == nil {
		return nil, nil
	}
	if s.paramFields == nil {
		fields := make([]*mysql.Field, len(s.RawParamFields))
		for i, raw := range s.RawParamFields {
			field := &mysql.Field{}
			if err := field.Parse(raw); err != nil {
				return nil, err
			}
			fields[i] = field
		}
		s.paramFields = fields
	}
	return s.paramFields, nil
}

func (s *PreparedStmt) GetColumnFields() ([]*mysql.Field, error) {
	if s.RawColumnFields == nil {
		return nil, nil
	}
	if s.columnFields == nil {
		fields := make([]*mysql.Field, len(s.RawColumnFields))
		for i, raw := range s.RawColumnFields {
			field := &mysql.Field{}
			if err := field.Parse(raw); err != nil {
				return nil, err
			}
			fields[i] = field
		}
		s.columnFields = fields
	}
	return s.columnFields, nil
}
