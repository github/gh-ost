package mysql

// TypedBytes preserves the original MySQL type alongside the raw bytes
// for binary protocol parameters that are length-encoded.
type TypedBytes struct {
	Type  byte   // Original MySQL type
	Bytes []byte // Raw bytes
}
