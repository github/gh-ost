/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	"fmt"
	"net"
)

// ConnectionConfig is the minimal configuration required to connect to a MySQL server
type ConnectionConfig struct {
	Key        InstanceKey
	User       string
	Password   string
	ImpliedKey *InstanceKey
}

func NewConnectionConfig() *ConnectionConfig {
	config := &ConnectionConfig{
		Key: InstanceKey{},
	}
	config.ImpliedKey = &config.Key
	return config
}

// DuplicateCredentials creates a new connection config with given key and with same credentials as this config
func (this *ConnectionConfig) DuplicateCredentials(key InstanceKey) *ConnectionConfig {
	config := &ConnectionConfig{
		Key:      key,
		User:     this.User,
		Password: this.Password,
	}
	config.ImpliedKey = &config.Key
	return config
}

func (this *ConnectionConfig) Duplicate() *ConnectionConfig {
	return this.DuplicateCredentials(this.Key)
}

func (this *ConnectionConfig) String() string {
	return fmt.Sprintf("%s, user=%s", this.Key.DisplayString(), this.User)
}

func (this *ConnectionConfig) Equals(other *ConnectionConfig) bool {
	return this.Key.Equals(&other.Key) || this.ImpliedKey.Equals(other.ImpliedKey)
}

func (this *ConnectionConfig) GetDBUri(databaseName string) string {
	hostname := this.Key.Hostname
	var ip = net.ParseIP(hostname)
	if (ip != nil) && (ip.To4() == nil) {
		// Wrap IPv6 literals in square brackets
		hostname = fmt.Sprintf("[%s]", hostname)
	}
	interpolateParams := true
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?interpolateParams=%t&autocommit=true&charset=utf8mb4,utf8,latin1", this.User, this.Password, hostname, this.Key.Port, databaseName, interpolateParams)
}
