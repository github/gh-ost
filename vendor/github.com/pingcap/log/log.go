// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package log

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"gopkg.in/natefinch/lumberjack.v2"
)

var globalMu sync.Mutex
var globalLogger, subGlobalLogger, globalProperties, globalSugarLogger atomic.Value

var registerOnce sync.Once

func init() {
	conf := &Config{Level: "info", File: FileLogConfig{}}
	logger, props, _ := InitLogger(conf)
	ReplaceGlobals(logger, props)
}

// InitLogger initializes a zap logger.
func InitLogger(cfg *Config, opts ...zap.Option) (*zap.Logger, *ZapProperties, error) {
	var output zapcore.WriteSyncer
	var errOutput zapcore.WriteSyncer
	if len(cfg.File.Filename) > 0 {
		lg, err := initFileLog(&cfg.File)
		if err != nil {
			return nil, nil, err
		}
		if cfg.File.IsBuffered {
			output = &zapcore.BufferedWriteSyncer{
				WS:            zapcore.AddSync(lg),
				Size:          cfg.File.BufferSize,
				FlushInterval: cfg.File.BufferFlushInterval,
			}
		} else {
			output = zapcore.AddSync(lg)
		}
	} else {
		stdOut, _, err := zap.Open([]string{"stdout"}...)
		if err != nil {
			return nil, nil, err
		}
		output = stdOut
	}
	if len(cfg.ErrorOutputPath) > 0 {
		errOut, _, err := zap.Open([]string{cfg.ErrorOutputPath}...)
		if err != nil {
			return nil, nil, err
		}
		errOutput = errOut
	} else {
		errOutput = output
	}

	return InitLoggerWithWriteSyncer(cfg, output, errOutput, opts...)
}

func InitTestLogger(t zaptest.TestingT, cfg *Config, opts ...zap.Option) (*zap.Logger, *ZapProperties, error) {
	writer := newTestingWriter(t)
	zapOptions := []zap.Option{
		// Send zap errors to the same writer and mark the test as failed if
		// that happens.
		zap.ErrorOutput(writer.WithMarkFailed(true)),
	}
	opts = append(zapOptions, opts...)
	return InitLoggerWithWriteSyncer(cfg, writer, writer, opts...)
}

// InitLoggerWithWriteSyncer initializes a zap logger with specified write syncer.
func InitLoggerWithWriteSyncer(cfg *Config, output, errOutput zapcore.WriteSyncer, opts ...zap.Option) (*zap.Logger, *ZapProperties, error) {
	level := zap.NewAtomicLevel()
	err := level.UnmarshalText([]byte(cfg.Level))
	if err != nil {
		return nil, nil, err
	}
	encoder, err := NewTextEncoder(cfg)
	if err != nil {
		return nil, nil, err
	}
	registerOnce.Do(func() {
		err = zap.RegisterEncoder(ZapEncodingName, func(zapcore.EncoderConfig) (zapcore.Encoder, error) {
			return encoder, nil
		})
	})
	if err != nil {
		return nil, nil, err
	}
	if cfg.Timeout > 0 {
		output = LockWithTimeout(output, cfg.Timeout)
		errOutput = LockWithTimeout(errOutput, cfg.Timeout)
	}

	core := NewTextCore(encoder, output, level)
	opts = append(cfg.buildOptions(errOutput), opts...)
	lg := zap.New(core, opts...)
	r := &ZapProperties{
		Core:      core,
		Syncer:    output,
		ErrSyncer: errOutput,
		Level:     level,
	}
	return lg, r, nil
}

// LockWithTimeout wraps a WriteSyncer make it safe for concurrent use, just like zapcore.Lock()
// timeout seconds.
func LockWithTimeout(ws zapcore.WriteSyncer, timeout int) zapcore.WriteSyncer {
	r := &lockWithTimeoutWrapper{
		ws:      ws,
		lock:    make(chan struct{}, 1),
		t:       time.NewTicker(time.Second),
		timeout: timeout,
	}
	return r
}

type lockWithTimeoutWrapper struct {
	ws      zapcore.WriteSyncer
	lock    chan struct{}
	t       *time.Ticker
	timeout int
}

// getLockOrBlock returns true when get lock success, and false otherwise.
func (s *lockWithTimeoutWrapper) getLockOrBlock() bool {
	for i := 0; i < s.timeout; {
		select {
		case s.lock <- struct{}{}:
			return true
		case <-s.t.C:
			i++
		}
	}
	return false
}

func (s *lockWithTimeoutWrapper) unlock() {
	<-s.lock
}

func (s *lockWithTimeoutWrapper) Write(bs []byte) (int, error) {
	succ := s.getLockOrBlock()
	if !succ {
		panic(fmt.Sprintf("Timeout of %ds when trying to write log", s.timeout))
	}
	defer s.unlock()

	return s.ws.Write(bs)
}

func (s *lockWithTimeoutWrapper) Sync() error {
	succ := s.getLockOrBlock()
	if !succ {
		panic(fmt.Sprintf("Timeout of %ds when trying to sync log", s.timeout))
	}
	defer s.unlock()

	return s.ws.Sync()
}

// initFileLog initializes file based logging options.
func initFileLog(cfg *FileLogConfig) (*lumberjack.Logger, error) {
	// Verify file permissions early to prevent silent failures.
	// Since lumberjack creates log files lazily, permission issues wouldn't be detected
	// until the first write attempt. This is problematic when tools redirect their
	// error output to these log files.
	// e.g. dumpling use the log file as ErrorOutputPath, but the log file will not be created
	// if there has permission issues, and dumpling will not print any error message.

	// Create the directory if it doesn't exist
	dir := filepath.Dir(cfg.Filename)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("cannot create log directory: %w", err)
	}

	// Check if the path is a directory which is invalid
	if st, err := os.Stat(cfg.Filename); err == nil {
		if st.IsDir() {
			return nil, errors.New("can't use directory as log file name")
		}

		// Check if the file is writable
		file, err := os.OpenFile(cfg.Filename, os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return nil, fmt.Errorf("can't write to log file: %w", err)
		}
		file.Close()
	} else if os.IsNotExist(err) {
		// File doesn't exist, verify we can create it
		file, err := os.Create(cfg.Filename)
		if err != nil {
			return nil, fmt.Errorf("can't create log file: %w", err)
		}
		file.Close()
		// Remove the empty file since lumberjack will create it
		os.Remove(cfg.Filename)
	} else {
		return nil, fmt.Errorf("error checking log file: %w", err)
	}

	if cfg.MaxSize == 0 {
		cfg.MaxSize = defaultLogMaxSize
	}

	compress := false
	switch cfg.Compression {
	case "":
		compress = false
	case "gzip":
		compress = true
	default:
		return nil, fmt.Errorf("can't set compression to `%s`", cfg.Compression)
	}

	// use lumberjack to logrotate
	return &lumberjack.Logger{
		Filename:   cfg.Filename,
		MaxSize:    cfg.MaxSize,
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxDays,
		LocalTime:  true,
		Compress:   compress,
	}, nil
}

// ll returns the sub global logger, which has 'zap.AddCallerSkip(1)' than the global logger.
// It's safe for concurrent use.
func ll() *zap.Logger {
	return subGlobalLogger.Load().(*zap.Logger)
}

// L returns the global Logger, which can be reconfigured with ReplaceGlobals.
// It's safe for concurrent use.
func L() *zap.Logger {
	return globalLogger.Load().(*zap.Logger)
}

// S returns the global SugaredLogger, which can be reconfigured with
// ReplaceGlobals. It's safe for concurrent use.
func S() *zap.SugaredLogger {
	return globalSugarLogger.Load().(*zap.SugaredLogger)
}

// ReplaceGlobals replaces the global Logger and SugaredLogger, and returns a
// function to restore the original values. It's safe for concurrent use.
// Be careful when using this with buffered logger, the flush goroutine in
// https://pkg.go.dev/go.uber.org/zap/zapcore#BufferedWriteSyncer will not be stopped.
func ReplaceGlobals(logger *zap.Logger, props *ZapProperties) func() {
	// TODO: This globalMu can be replaced by atomic.Swap(), available since go1.17.
	globalMu.Lock()
	prevLogger := globalLogger.Load()
	prevProps := globalProperties.Load()
	globalLogger.Store(logger)
	subGlobalLogger.Store(logger.WithOptions(zap.AddCallerSkip(1)))
	globalSugarLogger.Store(logger.Sugar())
	globalProperties.Store(props)
	globalMu.Unlock()

	if prevLogger == nil || prevProps == nil {
		// When `ReplaceGlobals` is called first time, atomic.Value is empty.
		return func() {}
	}
	return func() {
		ReplaceGlobals(prevLogger.(*zap.Logger), prevProps.(*ZapProperties))
	}
}

// Sync flushes any buffered log entries.
func Sync() error {
	err := L().Sync()
	if err != nil {
		return err
	}
	return S().Sync()
}
