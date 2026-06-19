package statsd

import (
	"os"
	"sync"
	"unicode"
)

// ddExternalEnvVarName specifies the env var to inject the environment name.
const ddExternalEnvVarName = "DD_EXTERNAL_ENV"

var (
	externalEnv   = ""
	externalEnvMu sync.RWMutex // Protects concurrent access to externalEnv
)

// initExternalEnv initializes the external environment name.
func initExternalEnv() {
	var value = os.Getenv(ddExternalEnvVarName)
	if value != "" {
		externalEnvMu.Lock()
		externalEnv = sanitizeExternalEnv(value)
		externalEnvMu.Unlock()
	}
}

// sanitizeExternalEnv removes non-printable characters and pipe characters from the external environment name.
func sanitizeExternalEnv(externalEnv string) string {
	if externalEnv == "" {
		return ""
	}
	var output string
	for _, r := range externalEnv {
		if unicode.IsPrint(r) && r != '|' {
			output += string(r)
		}
	}

	return output
}

func getExternalEnv() string {
	externalEnvMu.RLock()
	defer externalEnvMu.RUnlock()
	return externalEnv
}
