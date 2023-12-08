/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	"crypto/tls"
	"testing"

	"github.com/openark/golib/log"
	test "github.com/openark/golib/tests"
)

const (
	transactionIsolation = "REPEATABLE-READ"
)

func init() {
	log.SetLevel(log.ERROR)
}

func TestNewConnectionConfig(t *testing.T) {
	c := NewConnectionConfig()
	test.S(t).ExpectEquals(c.Key.Hostname, "")
	test.S(t).ExpectEquals(c.Key.Port, 0)
	test.S(t).ExpectEquals(c.ImpliedKey.Hostname, "")
	test.S(t).ExpectEquals(c.ImpliedKey.Port, 0)
	test.S(t).ExpectEquals(c.User, "")
	test.S(t).ExpectEquals(c.Password, "")
	test.S(t).ExpectEquals(c.TransactionIsolation, "")
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

	dup := c.DuplicateCredentials(InstanceKey{Hostname: "otherhost", Port: 3310})
	test.S(t).ExpectEquals(dup.Key.Hostname, "otherhost")
	test.S(t).ExpectEquals(dup.Key.Port, 3310)
	test.S(t).ExpectEquals(dup.ImpliedKey.Hostname, "otherhost")
	test.S(t).ExpectEquals(dup.ImpliedKey.Port, 3310)
	test.S(t).ExpectEquals(dup.User, "gromit")
	test.S(t).ExpectEquals(dup.Password, "penguin")
	test.S(t).ExpectEquals(dup.tlsConfig, c.tlsConfig)
	test.S(t).ExpectEquals(dup.TransactionIsolation, c.TransactionIsolation)
}

func TestDuplicate(t *testing.T) {
	c := NewConnectionConfig()
	c.Key = InstanceKey{Hostname: "myhost", Port: 3306}
	c.User = "gromit"
	c.Password = "penguin"
	c.TransactionIsolation = transactionIsolation

	dup := c.Duplicate()
	test.S(t).ExpectEquals(dup.Key.Hostname, "myhost")
	test.S(t).ExpectEquals(dup.Key.Port, 3306)
	test.S(t).ExpectEquals(dup.ImpliedKey.Hostname, "myhost")
	test.S(t).ExpectEquals(dup.ImpliedKey.Port, 3306)
	test.S(t).ExpectEquals(dup.User, "gromit")
	test.S(t).ExpectEquals(dup.Password, "penguin")
	test.S(t).ExpectEquals(dup.TransactionIsolation, transactionIsolation)
}

func TestGetDBUri(t *testing.T) {
	c := NewConnectionConfig()
	c.Key = InstanceKey{Hostname: "myhost", Port: 3306}
	c.User = "gromit"
	c.Password = "penguin"
	c.Timeout = 1.2345
	c.TransactionIsolation = transactionIsolation

	uri := c.GetDBUri("test")
	test.S(t).ExpectEquals(uri, `gromit:penguin@tcp(myhost:3306)/test?autocommit=true&charset=utf8mb4,utf8,latin1&interpolateParams=true&tls=false&transaction_isolation="REPEATABLE-READ"&timeout=1.234500s&readTimeout=1.234500s&writeTimeout=1.234500s`)
}

func TestGetDBUriWithTLSSetup(t *testing.T) {
	c := NewConnectionConfig()
	c.Key = InstanceKey{Hostname: "myhost", Port: 3306}
	c.User = "gromit"
	c.Password = "penguin"
	c.Timeout = 1.2345
	c.tlsConfig = &tls.Config{}
	c.TransactionIsolation = transactionIsolation

	uri := c.GetDBUri("test")
	test.S(t).ExpectEquals(uri, `gromit:penguin@tcp(myhost:3306)/test?autocommit=true&charset=utf8mb4,utf8,latin1&interpolateParams=true&tls=ghost&transaction_isolation="REPEATABLE-READ"&timeout=1.234500s&readTimeout=1.234500s&writeTimeout=1.234500s`)
}
