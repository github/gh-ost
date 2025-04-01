package utils

import (
	"math"
	"unsafe"
)

func StringToByteSlice(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func ByteSliceToString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func Uint64ToInt64(val uint64) int64 {
	return int64(val)
}

func Uint64ToFloat64(val uint64) float64 {
	return math.Float64frombits(val)
}

func Int64ToUint64(val int64) uint64 {
	return uint64(val)
}

func Float64ToUint64(val float64) uint64 {
	return math.Float64bits(val)
}
