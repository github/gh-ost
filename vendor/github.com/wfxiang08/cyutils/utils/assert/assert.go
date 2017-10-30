// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package assert

import (
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
)

func Must(b bool) {
	if b {
		return
	}
	log.Panic("assertion failed")
}

func MustNoError(err error) {
	if err == nil {
		return
	}
	log.PanicError(err, "error happens, assertion failed")
}
