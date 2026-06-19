package mysql

// Result should be created by NewResultWithoutRows or NewResult. The zero value
// of Result is invalid.
type Result struct {
	Status   uint16
	Warnings uint16

	InsertId     uint64 //nolint:revive // exported field renamed would be a breaking API change
	AffectedRows uint64

	StatusMessage   string
	SessionTracking *SessionTrackingInfo

	*Resultset

	StreamResult *StreamResult
}

type SessionTrackingInfo struct {
	GTID             string
	TransactionState string
	Variables        map[string]string
	Schema           string
	State            string
	Characteristics  string
}

func NewResult(resultset *Resultset) *Result {
	return &Result{
		Resultset: resultset,
	}
}

func NewResultReserveResultset(fieldCount int) *Result {
	return &Result{
		Resultset: NewResultset(fieldCount),
	}
}

type Executer interface {
	Execute(query string, args ...any) (*Result, error)
}

func (r *Result) Close() {
	if r.Resultset != nil {
		r.returnToPool()
		r.Resultset = nil
	}
	if r.StreamResult != nil {
		r.StreamResult.Close()
		r.StreamResult = nil
	}
}

func (r *Result) HasResultset() bool {
	if r == nil {
		return false
	}
	if r.Resultset != nil && len(r.Fields) > 0 {
		return true
	}
	return false
}

func (r *Result) IsStreaming() bool {
	return r != nil && r.StreamResult != nil
}
