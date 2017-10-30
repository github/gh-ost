// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package trace

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
)

const (
	tab = "    "
)

type Record struct {
	Name string
	File string
	Line int
}

func (r *Record) String() string {
	if r == nil {
		return "[nil-record]"
	}
	return fmt.Sprintf("%s:%d %s", r.File, r.Line, r.Name)
}

type Stack []*Record

func Trace() Stack {
	return TraceN(1, 32)
}

func (s Stack) String() string {
	return s.StringWithIndent(0)
}

func (s Stack) StringWithIndent(indent int) string {
	var b bytes.Buffer
	for i, r := range s {
		for j := 0; j < indent; j++ {
			fmt.Fprint(&b, tab)
		}
		fmt.Fprintf(&b, "%-3d %s:%d\n", len(s)-i-1, r.File, r.Line)
		for j := 0; j < indent; j++ {
			fmt.Fprint(&b, tab)
		}
		fmt.Fprint(&b, tab, tab)
		fmt.Fprint(&b, r.Name, "\n")
	}
	if len(s) != 0 {
		for j := 0; j < indent; j++ {
			fmt.Fprint(&b, tab)
		}
		fmt.Fprint(&b, tab, "... ...\n")
	}
	return b.String()
}

func TraceN(skip, depth int) Stack {
	s := make([]*Record, 0, depth)
	for i := 0; i < depth; i++ {
		r := Caller(skip + i + 1)
		if r == nil {
			break
		}
		s = append(s, r)
	}
	return s
}

func Caller(skip int) *Record {
	pc, file, line, ok := runtime.Caller(skip + 1)
	if !ok {
		return nil
	}
	fn := runtime.FuncForPC(pc)
	if fn == nil || strings.HasPrefix(fn.Name(), "runtime.") {
		return nil
	}
	return &Record{
		Name: fn.Name(),
		File: file,
		Line: line,
	}
}
