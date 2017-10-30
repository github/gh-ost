// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package rolling_log

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"

	"github.com/wfxiang08/cyutils/utils/errors"
	"github.com/wfxiang08/cyutils/utils/trace"
	"strings"
	"syscall"
	"time"
)

const (
	Ldate         = log.Ldate
	Llongfile     = log.Llongfile
	Lmicroseconds = log.Lmicroseconds
	Lshortfile    = log.Lshortfile
	LstdFlags     = log.LstdFlags
	Ltime         = log.Ltime
)

type (
	LogType  int64
	LogLevel int64
)

const (
	TYPE_ERROR = LogType(1 << iota)
	TYPE_WARN
	TYPE_INFO
	TYPE_DEBUG
	TYPE_PANIC = LogType(^0)
)

const (
	LEVEL_NONE = LogLevel(1<<iota - 1)
	LEVEL_ERROR
	LEVEL_WARN
	LEVEL_INFO
	LEVEL_DEBUG
	LEVEL_ALL = LEVEL_DEBUG
)

func (t LogType) String() string {
	switch t {
	default:
		return "[LOG]"
	case TYPE_PANIC:
		return "[PANIC]"
	case TYPE_ERROR:
		return "[ERROR]"
	case TYPE_WARN:
		return "[WARN]"
	case TYPE_INFO:
		return "[INFO]"
	case TYPE_DEBUG:
		return "[DEBUG]"
	}
}

func (l *LogLevel) Set(v LogLevel) {
	atomic.StoreInt64((*int64)(l), int64(v))
}

func (l *LogLevel) Test(m LogType) bool {
	v := atomic.LoadInt64((*int64)(l))
	return (v & int64(m)) != 0
}

type nopCloser struct {
	io.Writer
}

func (*nopCloser) Close() error {
	return nil
}

func NopCloser(w io.Writer) io.WriteCloser {
	return &nopCloser{w}
}

type Logger struct {
	mu    sync.Mutex
	out   io.WriteCloser
	log   *log.Logger
	level LogLevel
	trace LogLevel
}

var StdLog = New(NopCloser(os.Stderr), "")

func New(writer io.Writer, prefix string) *Logger {
	out, ok := writer.(io.WriteCloser)
	if !ok {
		out = NopCloser(writer)
	}
	return &Logger{
		out:   out,
		log:   log.New(out, prefix, LstdFlags),
		level: LEVEL_ALL,
		trace: LEVEL_ERROR,
	}
}

func OpenFile(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0666)
	return f, errors.Trace(err)
}

func MustOpenFile(path string) *os.File {
	f, err := OpenFile(path)
	if err != nil {
		PanicErrorf(err, "open file log '%s' failed", path)
	}
	return f
}

func FileLog(path string) (*Logger, error) {
	f, err := OpenFile(path)
	if err != nil {
		return nil, err
	}
	return New(f, ""), nil
}

func MustFileLog(path string) *Logger {
	return New(MustOpenFile(path), "")
}

func (l *Logger) Flags() int {
	return l.log.Flags()
}

func (l *Logger) Prefix() string {
	return l.log.Prefix()
}

func (l *Logger) SetFlags(flags int) {
	l.log.SetFlags(flags)
}

func (l *Logger) SetPrefix(prefix string) {
	l.log.SetPrefix(prefix)
}

func (l *Logger) SetLevel(v LogLevel) {
	l.level.Set(v)
}

func (l *Logger) SetTraceLevel(v LogLevel) {
	l.trace.Set(v)
}

func (l *Logger) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.out.Close()
}

func (l *Logger) isDisabled(t LogType) bool {
	return t != TYPE_PANIC && !l.level.Test(t)
}

func (l *Logger) isTraceEnabled(t LogType) bool {
	return t == TYPE_PANIC || l.trace.Test(t)
}

func (l *Logger) Panic(v ...interface{}) {
	t := TYPE_PANIC
	s := fmt.Sprint(v...)
	l.output(1, nil, t, s)
	os.Exit(1)
}

func (l *Logger) Panicf(format string, v ...interface{}) {
	t := TYPE_PANIC
	s := fmt.Sprintf(format, v...)
	l.output(1, nil, t, s)
	os.Exit(1)
}

func (l *Logger) PanicError(err error, v ...interface{}) {
	t := TYPE_PANIC
	s := fmt.Sprint(v...)
	l.output(1, err, t, s)
	os.Exit(1)
}

func (l *Logger) PanicErrorf(err error, format string, v ...interface{}) {
	t := TYPE_PANIC
	s := fmt.Sprintf(format, v...)
	l.output(1, err, t, s)
	os.Exit(1)
}

func (l *Logger) Error(v ...interface{}) {
	t := TYPE_ERROR
	if l.isDisabled(t) {
		return
	}
	s := fmt.Sprint(v...)
	l.output(1, nil, t, s)
}

func (l *Logger) Errorf(format string, v ...interface{}) {
	t := TYPE_ERROR
	if l.isDisabled(t) {
		return
	}
	s := fmt.Sprintf(format, v...)
	l.output(1, nil, t, s)
}

func (l *Logger) ErrorError(err error, v ...interface{}) {
	t := TYPE_ERROR
	if l.isDisabled(t) {
		return
	}
	s := fmt.Sprint(v...)
	l.output(1, err, t, s)
}

func (l *Logger) ErrorErrorf(err error, format string, v ...interface{}) {
	t := TYPE_ERROR
	if l.isDisabled(t) {
		return
	}
	s := fmt.Sprintf(format, v...)
	l.output(1, err, t, s)
}

func (l *Logger) Warn(v ...interface{}) {
	t := TYPE_WARN
	if l.isDisabled(t) {
		return
	}
	s := fmt.Sprint(v...)
	l.output(1, nil, t, s)
}

func (l *Logger) Warnf(format string, v ...interface{}) {
	t := TYPE_WARN
	if l.isDisabled(t) {
		return
	}
	s := fmt.Sprintf(format, v...)
	l.output(1, nil, t, s)
}

func (l *Logger) WarnError(err error, v ...interface{}) {
	t := TYPE_WARN
	if l.isDisabled(t) {
		return
	}
	s := fmt.Sprint(v...)
	l.output(1, err, t, s)
}

func (l *Logger) WarnErrorf(err error, format string, v ...interface{}) {
	t := TYPE_WARN
	if l.isDisabled(t) {
		return
	}
	s := fmt.Sprintf(format, v...)
	l.output(1, err, t, s)
}

func (l *Logger) Info(v ...interface{}) {
	t := TYPE_INFO
	if l.isDisabled(t) {
		return
	}
	s := fmt.Sprint(v...)
	l.output(1, nil, t, s)
}

func (l *Logger) Infof(format string, v ...interface{}) {
	t := TYPE_INFO
	if l.isDisabled(t) {
		return
	}
	s := fmt.Sprintf(format, v...)
	l.output(1, nil, t, s)
}

func (l *Logger) InfoError(err error, v ...interface{}) {
	t := TYPE_INFO
	if l.isDisabled(t) {
		return
	}
	s := fmt.Sprint(v...)
	l.output(1, err, t, s)
}

func (l *Logger) InfoErrorf(err error, format string, v ...interface{}) {
	t := TYPE_INFO
	if l.isDisabled(t) {
		return
	}
	s := fmt.Sprintf(format, v...)
	l.output(1, err, t, s)
}

func (l *Logger) Debug(v ...interface{}) {
	t := TYPE_DEBUG
	if l.isDisabled(t) {
		return
	}
	s := fmt.Sprint(v...)
	l.output(1, nil, t, s)
}

func (l *Logger) Debugf(format string, v ...interface{}) {
	t := TYPE_DEBUG
	if l.isDisabled(t) {
		return
	}
	s := fmt.Sprintf(format, v...)
	l.output(1, nil, t, s)
}

func (l *Logger) DebugError(err error, v ...interface{}) {
	t := TYPE_DEBUG
	if l.isDisabled(t) {
		return
	}
	s := fmt.Sprint(v...)
	l.output(1, err, t, s)
}

func (l *Logger) DebugErrorf(err error, format string, v ...interface{}) {
	t := TYPE_DEBUG
	if l.isDisabled(t) {
		return
	}
	s := fmt.Sprintf(format, v...)
	l.output(1, err, t, s)
}

func (l *Logger) Print(v ...interface{}) {
	s := fmt.Sprint(v...)
	l.output(1, nil, 0, s)
}

func (l *Logger) Printf(format string, v ...interface{}) {
	s := fmt.Sprintf(format, v...)
	l.output(1, nil, 0, s)
}

func (l *Logger) Println(v ...interface{}) {
	s := fmt.Sprintln(v...)
	l.output(1, nil, 0, s)
}

func (l *Logger) output(traceskip int, err error, t LogType, s string) error {
	var stack trace.Stack
	if l.isTraceEnabled(t) {
		stack = trace.TraceN(traceskip+1, 32)
	}

	var b bytes.Buffer
	fmt.Fprint(&b, t, " ", s)

	if len(s) == 0 || s[len(s)-1] != '\n' {
		fmt.Fprint(&b, "\n")
	}

	if err != nil {
		fmt.Fprint(&b, "[error]: ", err.Error(), "\n")
		if stack := errors.Stack(err); stack != nil {
			fmt.Fprint(&b, stack.StringWithIndent(1))
		}
	}
	if len(stack) != 0 {
		fmt.Fprint(&b, "[stack]: \n", stack.StringWithIndent(1))
	}

	s = b.String()
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.log.Output(traceskip+2, s)
}

func Flags() int {
	return StdLog.log.Flags()
}

func Prefix() string {
	return StdLog.log.Prefix()
}

func SetFlags(flags int) {
	StdLog.log.SetFlags(flags)
}

func SetPrefix(prefix string) {
	StdLog.log.SetPrefix(prefix)
}

func SetLevel(v LogLevel) {
	StdLog.level.Set(v)
}

func SetTrace(v LogLevel) {
	StdLog.trace.Set(v)
}

func Panic(v ...interface{}) {
	t := TYPE_PANIC
	s := fmt.Sprint(v...)
	StdLog.output(1, nil, t, s)
	os.Exit(1)
}

func Panicf(format string, v ...interface{}) {
	t := TYPE_PANIC
	s := fmt.Sprintf(format, v...)
	StdLog.output(1, nil, t, s)
	os.Exit(1)
}

func PanicError(err error, v ...interface{}) {
	t := TYPE_PANIC
	s := fmt.Sprint(v...)
	StdLog.output(1, err, t, s)
	os.Exit(1)
}

func PanicErrorf(err error, format string, v ...interface{}) {
	t := TYPE_PANIC
	s := fmt.Sprintf(format, v...)
	StdLog.output(1, err, t, s)
	os.Exit(1)
}

func Error(v ...interface{}) {
	t := TYPE_ERROR
	if StdLog.isDisabled(t) {
		return
	}
	s := fmt.Sprint(v...)
	StdLog.output(1, nil, t, s)
}

func Errorf(format string, v ...interface{}) {
	t := TYPE_ERROR
	if StdLog.isDisabled(t) {
		return
	}
	s := fmt.Sprintf(format, v...)
	StdLog.output(1, nil, t, s)
}

func ErrorError(err error, v ...interface{}) {
	t := TYPE_ERROR
	if StdLog.isDisabled(t) {
		return
	}
	s := fmt.Sprint(v...)
	StdLog.output(1, err, t, s)
}

func ErrorErrorf(err error, format string, v ...interface{}) {
	t := TYPE_ERROR
	if StdLog.isDisabled(t) {
		return
	}
	s := fmt.Sprintf(format, v...)
	StdLog.output(1, err, t, s)
}

func Warn(v ...interface{}) {
	t := TYPE_WARN
	if StdLog.isDisabled(t) {
		return
	}
	s := fmt.Sprint(v...)
	StdLog.output(1, nil, t, s)
}

func Warnf(format string, v ...interface{}) {
	t := TYPE_WARN
	if StdLog.isDisabled(t) {
		return
	}
	s := fmt.Sprintf(format, v...)
	StdLog.output(1, nil, t, s)
}

func WarnError(err error, v ...interface{}) {
	t := TYPE_WARN
	if StdLog.isDisabled(t) {
		return
	}
	s := fmt.Sprint(v...)
	StdLog.output(1, err, t, s)
}

func WarnErrorf(err error, format string, v ...interface{}) {
	t := TYPE_WARN
	if StdLog.isDisabled(t) {
		return
	}
	s := fmt.Sprintf(format, v...)
	StdLog.output(1, err, t, s)
}

func Info(v ...interface{}) {
	t := TYPE_INFO
	if StdLog.isDisabled(t) {
		return
	}
	s := fmt.Sprint(v...)
	StdLog.output(1, nil, t, s)
}

func Infof(format string, v ...interface{}) {
	t := TYPE_INFO
	if StdLog.isDisabled(t) {
		return
	}
	s := fmt.Sprintf(format, v...)
	StdLog.output(1, nil, t, s)
}

func InfoError(err error, v ...interface{}) {
	t := TYPE_INFO
	if StdLog.isDisabled(t) {
		return
	}
	s := fmt.Sprint(v...)
	StdLog.output(1, err, t, s)
}

func InfoErrorf(err error, format string, v ...interface{}) {
	t := TYPE_INFO
	if StdLog.isDisabled(t) {
		return
	}
	s := fmt.Sprintf(format, v...)
	StdLog.output(1, err, t, s)
}

func Debug(v ...interface{}) {
	t := TYPE_DEBUG
	if StdLog.isDisabled(t) {
		return
	}
	s := fmt.Sprint(v...)
	StdLog.output(1, nil, t, s)
}

func Debugf(format string, v ...interface{}) {
	t := TYPE_DEBUG
	if StdLog.isDisabled(t) {
		return
	}
	s := fmt.Sprintf(format, v...)
	StdLog.output(1, nil, t, s)
}

func DebugError(err error, v ...interface{}) {
	t := TYPE_DEBUG
	if StdLog.isDisabled(t) {
		return
	}
	s := fmt.Sprint(v...)
	StdLog.output(1, err, t, s)
}

func DebugErrorf(err error, format string, v ...interface{}) {
	t := TYPE_DEBUG
	if StdLog.isDisabled(t) {
		return
	}
	s := fmt.Sprintf(format, v...)
	StdLog.output(1, err, t, s)
}

func Print(v ...interface{}) {
	s := fmt.Sprint(v...)
	StdLog.output(1, nil, 0, s)
}

func Printf(format string, v ...interface{}) {
	s := fmt.Sprintf(format, v...)
	StdLog.output(1, nil, 0, s)
}

func Println(v ...interface{}) {
	s := fmt.Sprintln(v...)
	StdLog.output(1, nil, 0, s)
}

//
// 设置LogLevel
//
func SetLogLevel(level string) {
	var lv = LEVEL_INFO
	switch strings.ToLower(level) {
	case "error":
		lv = LEVEL_ERROR
	case "warn", "warning":
		lv = LEVEL_WARN
	case "debug":
		lv = LEVEL_DEBUG
	case "info":
		fallthrough
	default:
		lv = LEVEL_INFO
	}
	SetLevel(lv)
	Infof("set log level to %s", level)
}

//
// 设置CrashLog
//
func SetCrashLog(file string) {
	f, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		InfoErrorf(err, "cannot open crash log file: %s", file)
	} else {
		syscall.Dup2(int(f.Fd()), 2)
	}
}

func FormatYYYYmmDDHHMMSS(date time.Time) string {
	return date.Format("2006-01-02 15:04:05")
}
