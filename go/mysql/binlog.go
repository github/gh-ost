/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import "errors"

type BinlogCoordinates interface {
	String() string
	DisplayString() string
	IsEmpty() bool
	Equals(other BinlogCoordinates) bool
	SmallerThan(other BinlogCoordinates) bool
	SmallerThanOrEquals(other BinlogCoordinates) bool
}

func binlogCoordinatesToImplementation(in BinlogCoordinates, out interface{}) (err error) {
	var ok bool
	switch out.(type) {
	case *FileBinlogCoordinates:
		out, ok = in.(*FileBinlogCoordinates)
	case *GTIDBinlogCoordinates:
		out, ok = in.(*GTIDBinlogCoordinates)
	default:
		err = errors.New("unrecognized BinlogCoordinates implementation")
	}

	if !ok {
		err = errors.New("failed to reflect BinlogCoordinates implementation")
	}
	return err
}
