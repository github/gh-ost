package log

import (
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"time"
)

// LogLevel indicates the severity of a log entry.
type LogLevel int

const (
	FATAL LogLevel = iota
	CRITICAL
	ERROR
	WARNING
	NOTICE
	INFO
	DEBUG
)

const timeFormat = "2006-01-02 15:04:05"

var globalLogLevel = DEBUG
var printStackTrace bool

func (level LogLevel) String() string {
	switch level {
	case FATAL:
		return "FATAL"
	case CRITICAL:
		return "CRITICAL"
	case ERROR:
		return "ERROR"
	case WARNING:
		return "WARNING"
	case NOTICE:
		return "NOTICE"
	case INFO:
		return "INFO"
	case DEBUG:
		return "DEBUG"
	default:
		return "unknown"
	}
}

// SetLevel sets the minimum severity emitted by the package logger.
func SetLevel(level LogLevel) {
	globalLogLevel = level
}

// SetPrintStackTrace enables or disables stack traces for error-object logging.
func SetPrintStackTrace(shouldPrintStackTrace bool) {
	printStackTrace = shouldPrintStackTrace
}

func logFormattedEntry(level LogLevel, message string, args ...interface{}) string {
	if level > globalLogLevel {
		return ""
	}

	localizedTime := time.Now()
	if locationName := os.Getenv("TZ"); locationName != "" {
		if location, err := time.LoadLocation(locationName); err == nil {
			localizedTime = localizedTime.In(location)
		}
	}

	entry := fmt.Sprintf("%s %s %s", localizedTime.Format(timeFormat), level, fmt.Sprintf(message, args...))
	fmt.Fprintln(os.Stderr, entry)
	return entry
}

func logEntry(level LogLevel, message string, args ...interface{}) string {
	entry := message
	for _, arg := range args {
		entry += fmt.Sprintf(" %s", arg)
	}
	return logFormattedEntry(level, "%s", entry)
}

func logErrorEntry(level LogLevel, err error) error {
	if err == nil {
		return nil
	}
	logEntry(level, fmt.Sprintf("%+v", err))
	if printStackTrace {
		debug.PrintStack()
	}
	return err
}

func Debug(message string, args ...interface{}) string {
	return logEntry(DEBUG, message, args...)
}

func Debugf(message string, args ...interface{}) string {
	return logFormattedEntry(DEBUG, message, args...)
}

func Info(message string, args ...interface{}) string {
	return logEntry(INFO, message, args...)
}

func Infof(message string, args ...interface{}) string {
	return logFormattedEntry(INFO, message, args...)
}

func Warning(message string, args ...interface{}) error {
	return errors.New(logEntry(WARNING, message, args...))
}

func Warningf(message string, args ...interface{}) error {
	return errors.New(logFormattedEntry(WARNING, message, args...))
}

func Error(message string, args ...interface{}) error {
	return errors.New(logEntry(ERROR, message, args...))
}

func Errorf(message string, args ...interface{}) error {
	return errors.New(logFormattedEntry(ERROR, message, args...))
}

func Errore(err error) error {
	return logErrorEntry(ERROR, err)
}

func Fatal(message string, args ...interface{}) error {
	logEntry(FATAL, message, args...)
	os.Exit(1)
	return nil
}

func Fatalf(message string, args ...interface{}) error {
	logFormattedEntry(FATAL, message, args...)
	os.Exit(1)
	return nil
}

func Fatale(err error) error {
	logErrorEntry(FATAL, err)
	os.Exit(1)
	return err
}
