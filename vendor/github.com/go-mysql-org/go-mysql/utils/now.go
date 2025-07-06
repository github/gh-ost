//go:build !unix

package utils

import "time"

var Now = time.Now
