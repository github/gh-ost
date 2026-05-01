/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/go-sql-driver/mysql"
)

const (
	TLS_CONFIG_KEY = "ghost"
)

// ConnectionConfig is the minimal configuration required to connect to a MySQL server
type ConnectionConfig struct {
	Key                  InstanceKey
	User                 string
	Password             string
	ImpliedKey           *InstanceKey
	tlsConfig            *tls.Config
	Timeout              float64
	TransactionIsolation string
	Charset              string
}

func NewConnectionConfig() *ConnectionConfig {
	config := &ConnectionConfig{
		Key: InstanceKey{},
	}
	config.ImpliedKey = &config.Key
	return config
}

// DuplicateCredentials creates a new connection config with given key and with same credentials as this config
func (con *ConnectionConfig) DuplicateCredentials(key InstanceKey) *ConnectionConfig {
	config := &ConnectionConfig{
		Key:                  key,
		User:                 con.User,
		Password:             con.Password,
		tlsConfig:            con.tlsConfig,
		Timeout:              con.Timeout,
		TransactionIsolation: con.TransactionIsolation,
		Charset:              con.Charset,
	}

	if con.tlsConfig != nil {
		config.tlsConfig = &tls.Config{
			ServerName:         key.Hostname,
			Certificates:       con.tlsConfig.Certificates,
			RootCAs:            con.tlsConfig.RootCAs,
			InsecureSkipVerify: con.tlsConfig.InsecureSkipVerify,
		}
	}

	config.ImpliedKey = &config.Key
	return config
}

func (con *ConnectionConfig) Duplicate() *ConnectionConfig {
	return con.DuplicateCredentials(con.Key)
}

func (con *ConnectionConfig) String() string {
	return fmt.Sprintf("%s, user=%s, usingTLS=%t", con.Key.DisplayString(), con.User, con.tlsConfig != nil)
}

func (con *ConnectionConfig) Equals(other *ConnectionConfig) bool {
	return con.Key.Equals(&other.Key) || con.ImpliedKey.Equals(other.ImpliedKey)
}

func (con *ConnectionConfig) UseTLS(caCertificatePath, clientCertificate, clientKey string, allowInsecure bool) error {
	var rootCertPool *x509.CertPool
	var certs []tls.Certificate
	var err error

	if caCertificatePath == "" {
		rootCertPool, err = x509.SystemCertPool()
		if err != nil {
			return err
		}
	} else {
		rootCertPool = x509.NewCertPool()
		pem, err := os.ReadFile(caCertificatePath)
		if err != nil {
			return err
		}
		if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
			return errors.New("could not add ca certificate to cert pool")
		}
	}
	if clientCertificate != "" || clientKey != "" {
		cert, err := tls.LoadX509KeyPair(clientCertificate, clientKey)
		if err != nil {
			return err
		}
		certs = []tls.Certificate{cert}
	}

	con.tlsConfig = &tls.Config{
		ServerName:         con.Key.Hostname,
		Certificates:       certs,
		RootCAs:            rootCertPool,
		InsecureSkipVerify: allowInsecure,
	}

	return con.RegisterTLSConfig()
}

func (con *ConnectionConfig) RegisterTLSConfig() error {
	if con.tlsConfig == nil {
		return nil
	}
	if con.tlsConfig.ServerName == "" {
		return errors.New("tlsConfig.ServerName cannot be empty")
	}

	var tlsOption = GetDBTLSConfigKey(con.tlsConfig.ServerName)

	return mysql.RegisterTLSConfig(tlsOption, con.tlsConfig)
}

func (con *ConnectionConfig) TLSConfig() *tls.Config {
	return con.tlsConfig
}

func (con *ConnectionConfig) GetDBUri(databaseName string) string {
	hostname := con.Key.Hostname
	var ip = net.ParseIP(hostname)
	if (ip != nil) && (ip.To4() == nil) {
		// Wrap IPv6 literals in square brackets
		hostname = fmt.Sprintf("[%s]", hostname)
	}

	// go-mysql-driver defaults to false if tls param is not provided; explicitly setting here to
	// simplify construction of the DSN below.
	tlsOption := "false"
	if con.tlsConfig != nil {
		tlsOption = GetDBTLSConfigKey(con.tlsConfig.ServerName)
	}

	if con.Charset == "" {
		con.Charset = "utf8mb4,utf8,latin1"
	}

	connectionParams := []string{
		"autocommit=true",
		"interpolateParams=true",
		fmt.Sprintf("charset=%s", con.Charset),
		fmt.Sprintf("tls=%s", tlsOption),
		fmt.Sprintf("transaction_isolation=%q", con.TransactionIsolation),
		fmt.Sprintf("timeout=%fs", con.Timeout),
		fmt.Sprintf("readTimeout=%fs", con.Timeout),
		fmt.Sprintf("writeTimeout=%fs", con.Timeout),
	}

	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", con.User, con.Password, hostname, con.Key.Port, databaseName, strings.Join(connectionParams, "&"))
}

func GetDBTLSConfigKey(tlsServerName string) string {
	return fmt.Sprintf("%s-%s", TLS_CONFIG_KEY, tlsServerName)
}
