/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	"crypto/tls"
	"testing"

	"github.com/openark/golib/log"
	"github.com/stretchr/testify/require"
)

const (
	transactionIsolation = "REPEATABLE-READ"
)

func init() {
	log.SetLevel(log.ERROR)
}

func TestNewConnectionConfig(t *testing.T) {
	c := NewConnectionConfig()
	require.Equal(t, "", c.Key.Hostname)
	require.Equal(t, 0, c.Key.Port)
	require.Equal(t, "", c.ImpliedKey.Hostname)
	require.Equal(t, 0, c.ImpliedKey.Port)
	require.Equal(t, "", c.User)
	require.Equal(t, "", c.Password)
	require.Equal(t, "", c.TransactionIsolation)
	require.Equal(t, "", c.Charset)
}

func TestDuplicateCredentials(t *testing.T) {
	c := NewConnectionConfig()
	c.Key = InstanceKey{Hostname: "myhost", Port: 3306}
	c.User = "gromit"
	c.Password = "penguin"
	c.tlsConfig = &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         "feathers",
	}
	c.TransactionIsolation = transactionIsolation
	c.Charset = "utf8mb4"

	dup := c.DuplicateCredentials(InstanceKey{Hostname: "otherhost", Port: 3310})
	require.Equal(t, "otherhost", dup.Key.Hostname)
	require.Equal(t, 3310, dup.Key.Port)
	require.Equal(t, "otherhost", dup.ImpliedKey.Hostname)
	require.Equal(t, 3310, dup.ImpliedKey.Port)
	require.Equal(t, "gromit", dup.User)
	require.Equal(t, "penguin", dup.Password)
	require.Equal(t, c.tlsConfig, dup.tlsConfig)
	require.Equal(t, c.TransactionIsolation, dup.TransactionIsolation)
	require.Equal(t, c.Charset, dup.Charset)
}

func TestDuplicate(t *testing.T) {
	c := NewConnectionConfig()
	c.Key = InstanceKey{Hostname: "myhost", Port: 3306}
	c.User = "gromit"
	c.Password = "penguin"
	c.TransactionIsolation = transactionIsolation
	c.Charset = "utf8mb4"

	dup := c.Duplicate()
	require.Equal(t, "myhost", dup.Key.Hostname)
	require.Equal(t, 3306, dup.Key.Port)
	require.Equal(t, "myhost", dup.ImpliedKey.Hostname)
	require.Equal(t, 3306, dup.ImpliedKey.Port)
	require.Equal(t, "gromit", dup.User)
	require.Equal(t, "penguin", dup.Password)
	require.Equal(t, transactionIsolation, dup.TransactionIsolation)
	require.Equal(t, "utf8mb4", dup.Charset)
}

func TestGetDBUri(t *testing.T) {
	c := NewConnectionConfig()
	c.Key = InstanceKey{Hostname: "myhost", Port: 3306}
	c.User = "gromit"
	c.Password = "penguin"
	c.Timeout = 1.2345
	c.TransactionIsolation = transactionIsolation
	c.Charset = "utf8mb4,utf8,latin1"

	uri := c.GetDBUri("test")
	require.Equal(t, `gromit:penguin@tcp(myhost:3306)/test?autocommit=true&interpolateParams=true&charset=utf8mb4,utf8,latin1&tls=false&transaction_isolation="REPEATABLE-READ"&timeout=1.234500s&readTimeout=1.234500s&writeTimeout=1.234500s`, uri)
}

func TestGetDBUriWithTLSSetup(t *testing.T) {
	c := NewConnectionConfig()
	c.Key = InstanceKey{Hostname: "myhost", Port: 3306}
	c.User = "gromit"
	c.Password = "penguin"
	c.Timeout = 1.2345
	c.tlsConfig = &tls.Config{}
	c.TransactionIsolation = transactionIsolation
	c.Charset = "utf8mb4_general_ci,utf8_general_ci,latin1"

	uri := c.GetDBUri("test")
	require.Equal(t, `gromit:penguin@tcp(myhost:3306)/test?autocommit=true&interpolateParams=true&charset=utf8mb4_general_ci,utf8_general_ci,latin1&tls=ghost&transaction_isolation="REPEATABLE-READ"&timeout=1.234500s&readTimeout=1.234500s&writeTimeout=1.234500s`, uri)
}
