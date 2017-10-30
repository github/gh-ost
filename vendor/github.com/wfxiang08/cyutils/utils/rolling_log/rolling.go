// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package rolling_log

import (
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/wfxiang08/cyutils/utils/errors"
)

//  按天进行Rolling
type rollingFile struct {
	mu sync.Mutex

	logKeepDays     int
	basePath        string
	file            *os.File
	filePath        string
	nextStartOfDate time.Time
	closed          bool
}

func NewRollingFile(basePath string, logKeepDays int) (io.WriteCloser, error) {
	if logKeepDays <= 0 {
		return nil, errors.Errorf("invalid max file-frag = %d", logKeepDays)
	}
	if _, file := path.Split(basePath); file == "" {
		return nil, errors.Errorf("invalid base-path = %s, file name is required", basePath)
	}

	return &rollingFile{
		logKeepDays:     logKeepDays,
		basePath:        basePath,
		nextStartOfDate: nextStartOfDay(time.Now()),
	}, nil
}

var ErrClosedRollingFile = errors.New("rolling file is closed")

func (r *rollingFile) roll() error {
	if r.file != nil {
		// 还没有日期切换
		now := time.Now()
		if now.Before(r.nextStartOfDate) {
			return nil
		}
		r.file.Close()
		r.file = nil
		r.nextStartOfDate = nextStartOfDay(now)

		// 将logKeepDays之前几天的日志删除
		for i := r.logKeepDays; i < r.logKeepDays+4; i++ {

			filePath := fmt.Sprintf("%s-%s", r.basePath, now.AddDate(0, 0, -i).Format("20060102"))
			_, err := os.Stat(filePath)
			// 如果存在，则删除
			if !os.IsNotExist(err) {
				os.Remove(filePath)
			}
		}

	}

	// 例如: proxy.log-20140101
	r.filePath = fmt.Sprintf("%s-%s", r.basePath, time.Now().Format("20060102"))

	f, err := os.OpenFile(r.filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return errors.Trace(err)
	} else {
		r.file = f
		return nil
	}
}

func (r *rollingFile) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}

	r.closed = true
	if f := r.file; f != nil {
		r.file = nil
		return errors.Trace(f.Close())
	}
	return nil
}

func (r *rollingFile) Write(b []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return 0, errors.Trace(ErrClosedRollingFile)
	}

	if err := r.roll(); err != nil {
		return 0, err
	}

	n, err := r.file.Write(b)
	if err != nil {
		return n, errors.Trace(err)
	} else {
		return n, nil
	}
}

func nextStartOfDay(t time.Time) time.Time {
	year, month, day := t.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, t.Location()).AddDate(0, 0, 1)
}