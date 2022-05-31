/*
   Copyright 2022 GitHub Inc.
         See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package base

import (
	"github.com/openark/golib/log"
)

type simpleLogger struct{}

func NewDefaultLogger() *simpleLogger {
	return &simpleLogger{}
}

func (*simpleLogger) Debug(args ...interface{}) {
	log.Debug(args[0].(string), args[1:])
}

func (*simpleLogger) Debugf(format string, args ...interface{}) {
	log.Debugf(format, args...)
}

func (*simpleLogger) Info(args ...interface{}) {
	log.Info(args[0].(string), args[1:])
}

func (*simpleLogger) Infof(format string, args ...interface{}) {
	log.Infof(format, args...)
}

func (*simpleLogger) Warning(args ...interface{}) error {
	return log.Warning(args[0].(string), args[1:])
}

func (*simpleLogger) Warningf(format string, args ...interface{}) error {
	return log.Warningf(format, args...)
}

func (*simpleLogger) Error(args ...interface{}) error {
	return log.Error(args[0].(string), args[1:])
}

func (*simpleLogger) Errorf(format string, args ...interface{}) error {
	return log.Errorf(format, args...)
}

func (*simpleLogger) Errore(err error) error {
	return log.Errore(err)
}

func (*simpleLogger) Fatal(args ...interface{}) error {
	return log.Fatal(args[0].(string), args[1:])
}

func (*simpleLogger) Fatalf(format string, args ...interface{}) error {
	return log.Fatalf(format, args...)
}

func (*simpleLogger) Fatale(err error) error {
	return log.Fatale(err)
}

func (*simpleLogger) SetLevel(level log.LogLevel) {
	log.SetLevel(level)
}

func (*simpleLogger) SetPrintStackTrace(printStackTraceFlag bool) {
	log.SetPrintStackTrace(printStackTraceFlag)
}
