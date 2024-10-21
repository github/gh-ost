/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	"testing"

	"github.com/openark/golib/log"
	"github.com/stretchr/testify/require"
)

func init() {
	log.SetLevel(log.ERROR)
}

func TestParseInstanceKey(t *testing.T) {
	{
		key, err := ParseInstanceKey("myhost:1234")
		require.NoError(t, err)
		require.Equal(t, "myhost", key.Hostname)
		require.Equal(t, 1234, key.Port)
	}
	{
		key, err := ParseInstanceKey("myhost")
		require.NoError(t, err)
		require.Equal(t, "myhost", key.Hostname)
		require.Equal(t, 3306, key.Port)
	}
	{
		key, err := ParseInstanceKey("10.0.0.3:3307")
		require.NoError(t, err)
		require.Equal(t, "10.0.0.3", key.Hostname)
		require.Equal(t, 3307, key.Port)
	}
	{
		key, err := ParseInstanceKey("10.0.0.3")
		require.NoError(t, err)
		require.Equal(t, "10.0.0.3", key.Hostname)
		require.Equal(t, 3306, key.Port)
	}
	{
		key, err := ParseInstanceKey("[2001:db8:1f70::999:de8:7648:6e8]:3308")
		require.NoError(t, err)
		require.Equal(t, "2001:db8:1f70::999:de8:7648:6e8", key.Hostname)
		require.Equal(t, 3308, key.Port)
	}
	{
		key, err := ParseInstanceKey("::1")
		require.NoError(t, err)
		require.Equal(t, "::1", key.Hostname)
		require.Equal(t, 3306, key.Port)
	}
	{
		key, err := ParseInstanceKey("0:0:0:0:0:0:0:0")
		require.NoError(t, err)
		require.Equal(t, "0:0:0:0:0:0:0:0", key.Hostname)
		require.Equal(t, 3306, key.Port)
	}
	{
		_, err := ParseInstanceKey("[2001:xxxx:1f70::999:de8:7648:6e8]:3308")
		require.Error(t, err)
	}
	{
		_, err := ParseInstanceKey("10.0.0.4:")
		require.Error(t, err)
	}
	{
		_, err := ParseInstanceKey("10.0.0.4:5.6.7")
		require.Error(t, err)
	}
}
