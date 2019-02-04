/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"net"

	"github.com/go-sql-driver/mysql"
)

// ConnectionConfig is the minimal configuration required to connect to a MySQL server
type ConnectionConfig struct {
	Key        InstanceKey
	User       string
	Password   string
	ImpliedKey *InstanceKey
	tlsConfig  *tls.Config
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
		Key:       key,
		User:      this.User,
		Password:  this.Password,
		tlsConfig: this.tlsConfig,
	}
	config.ImpliedKey = &config.Key
	return config
}

func (this *ConnectionConfig) Duplicate() *ConnectionConfig {
	return this.DuplicateCredentials(this.Key)
}

func (this *ConnectionConfig) String() string {
	return fmt.Sprintf("%s, user=%s, usingTLS=%t", this.Key.DisplayString(), this.User, this.tlsConfig != nil)
}

func (this *ConnectionConfig) Equals(other *ConnectionConfig) bool {
	return this.Key.Equals(&other.Key) || this.ImpliedKey.Equals(other.ImpliedKey)
}

func (this *ConnectionConfig) UseTLS(caCertificatePath string, allowInsecure bool) error {
	var rootCertPool *x509.CertPool
	var err error

	if !allowInsecure {
		if caCertificatePath == "" {
			rootCertPool, err = x509.SystemCertPool()
			if err != nil {
				return err
			}
		} else {
			rootCertPool = x509.NewCertPool()
			pem, err := ioutil.ReadFile(caCertificatePath)
			if err != nil {
				return err
			}
			if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
				return errors.New("could not add ca certificate to cert pool")
			}
		}
	}

	this.tlsConfig = &tls.Config{
		RootCAs:            rootCertPool,
		InsecureSkipVerify: allowInsecure,
	}

	return mysql.RegisterTLSConfig(this.Key.StringCode(), this.tlsConfig)
}

func (this *ConnectionConfig) TLSConfig() *tls.Config {
	return this.tlsConfig
}

func (this *ConnectionConfig) GetDBUri(databaseName string) string {
	hostname := this.Key.Hostname
	var ip = net.ParseIP(hostname)
	if (ip != nil) && (ip.To4() == nil) {
		// Wrap IPv6 literals in square brackets
		hostname = fmt.Sprintf("[%s]", hostname)
	}
	interpolateParams := true
	// go-mysql-driver defaults to false if tls param is not provided; explicitly setting here to
	// simplify construction of the DSN below.
	tlsOption := "false"
	if this.tlsConfig != nil {
		tlsOption = this.Key.StringCode()
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?interpolateParams=%t&autocommit=true&charset=utf8mb4,utf8,latin1&tls=%s", this.User, this.Password, hostname, this.Key.Port, databaseName, interpolateParams, tlsOption)
}
