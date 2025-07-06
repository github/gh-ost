package client

import (
	"crypto/tls"
	"crypto/x509"
)

// NewClientTLSConfig: generate TLS config for client side
// if insecureSkipVerify is set to true, serverName will not be validated
func NewClientTLSConfig(caPem, certPem, keyPem []byte, insecureSkipVerify bool, serverName string) *tls.Config {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPem) {
		panic("failed to add ca PEM")
	}

	var config *tls.Config

	// Allow cert and key to be optional
	// Send through `make([]byte, 0)` for "nil"
	if string(certPem) != "" && string(keyPem) != "" {
		cert, err := tls.X509KeyPair(certPem, keyPem)
		if err != nil {
			panic(err)
		}
		config = &tls.Config{
			RootCAs:            pool,
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: insecureSkipVerify,
			ServerName:         serverName,
		}
	} else {
		config = &tls.Config{
			RootCAs:            pool,
			InsecureSkipVerify: insecureSkipVerify,
			ServerName:         serverName,
		}
	}

	return config
}
