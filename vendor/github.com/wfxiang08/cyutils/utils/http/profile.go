package http

import (
	"github.com/fatih/color"
	"github.com/wfxiang08/cyutils/utils/atomic2"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"net/http"
	_ "net/http/pprof"
	"time"
)

func StartHttpProfile(profileAddr string, running atomic2.Bool) {
	// 异步启动一个debug server
	// 由于可能多个进程同时存在，因此如果失败了，就等待重启
	log.Printf(color.RedString("Profile Address: %s"), profileAddr)
	for running.Get() {
		time.Sleep(time.Second * 10)
		err := http.ListenAndServe(profileAddr, nil)
		if err != nil {
			log.ErrorErrorf(err, "profile server start failed")
		} else {
			break
		}
	}
}
