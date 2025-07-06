package mysql

// Result should be created by NewResultWithoutRows or NewResult. The zero value
// of Result is invalid.
type Result struct {
	Status   uint16
	Warnings uint16

	InsertId     uint64
	AffectedRows uint64

	*Resultset
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
	Execute(query string, args ...interface{}) (*Result, error)
}

func (r *Result) Close() {
	if r.Resultset != nil {
		r.Resultset.returnToPool()
		r.Resultset = nil
	}
}
