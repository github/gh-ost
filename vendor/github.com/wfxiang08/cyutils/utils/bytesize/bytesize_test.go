// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package bytesize

import (
	"github.com/wfxiang08/cyutils/utils/assert"
	. "github.com/wfxiang08/cyutils/utils/bytesize"
	"github.com/wfxiang08/cyutils/utils/errors"
	"testing"
)

func TestBytesize(t *testing.T) {
	assert.Must(MustParse("1") == 1)
	assert.Must(MustParse("1B") == 1)
	assert.Must(MustParse("1K") == KB)
	assert.Must(MustParse("1M") == MB)
	assert.Must(MustParse("1G") == GB)
	assert.Must(MustParse("1T") == TB)
	assert.Must(MustParse("1P") == PB)

	assert.Must(MustParse(" -1") == -1)
	assert.Must(MustParse(" -1 b") == -1)
	assert.Must(MustParse(" -1 kb ") == -1*KB)
	assert.Must(MustParse(" -1 mb ") == -1*MB)
	assert.Must(MustParse(" -1 gb ") == -1*GB)
	assert.Must(MustParse(" -1 tb ") == -1*TB)
	assert.Must(MustParse(" -1 pb ") == -1*PB)

	assert.Must(MustParse(" 1.5") == 1)
	assert.Must(MustParse(" 1.5 kb ") == 1.5*KB)
	assert.Must(MustParse(" 1.5 mb ") == 1.5*MB)
	assert.Must(MustParse(" 1.5 gb ") == 1.5*GB)
	assert.Must(MustParse(" 1.5 tb ") == 1.5*TB)
	assert.Must(MustParse(" 1.5 pb ") == 1.5*PB)
}

func TestBytesizeError(t *testing.T) {
	var err error
	_, err = Parse("--1")
	assert.Must(errors.Equal(err, ErrBadBytesize))
	_, err = Parse("hello world")
	assert.Must(errors.Equal(err, ErrBadBytesize))
	_, err = Parse("123.132.32")
	assert.Must(errors.Equal(err, ErrBadBytesize))
}
