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

func TestParseLoadMap(t *testing.T) {
	{
		loadList := ""
		m, err := ParseLoadMap(loadList)
		require.NoError(t, err)
		require.Len(t, m, 0)
	}
	{
		loadList := "threads_running=20,threads_connected=10"
		m, err := ParseLoadMap(loadList)
		require.NoError(t, err)
		require.Len(t, m, 2)
		require.Equal(t, int64(20), m["threads_running"])
		require.Equal(t, int64(10), m["threads_connected"])
	}
	{
		loadList := "threads_running=20=30,threads_connected=10"
		_, err := ParseLoadMap(loadList)
		require.Error(t, err)
	}
	{
		loadList := "threads_running=20,threads_connected"
		_, err := ParseLoadMap(loadList)
		require.Error(t, err)
	}
}

func TestString(t *testing.T) {
	{
		m, _ := ParseLoadMap("")
		s := m.String()
		require.Equal(t, "", s)
	}
	{
		loadList := "threads_running=20,threads_connected=10"
		m, _ := ParseLoadMap(loadList)
		s := m.String()
		require.Equal(t, "threads_connected=10,threads_running=20", s)
	}
}
