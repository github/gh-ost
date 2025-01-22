/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package base

import (
	"testing"

	"github.com/openark/golib/log"
	"github.com/stretchr/testify/require"
)

func init() {
	log.SetLevel(log.ERROR)
}

func TestStringContainsAll(t *testing.T) {
	s := `insert,delete,update`

	require.False(t, StringContainsAll(s))
	require.False(t, StringContainsAll(s, ""))
	require.False(t, StringContainsAll(s, "drop"))
	require.True(t, StringContainsAll(s, "insert"))
	require.False(t, StringContainsAll(s, "insert", "drop"))
	require.True(t, StringContainsAll(s, "insert", ""))
	require.True(t, StringContainsAll(s, "insert", "update", "delete"))
}
