package log

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"
)

func captureStderr(t *testing.T, action func()) string {
	t.Helper()

	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	originalStderr := os.Stderr
	os.Stderr = writer
	t.Cleanup(func() {
		os.Stderr = originalStderr
	})

	action()
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return string(output)
}

func preserveGlobalState(t *testing.T) {
	t.Helper()
	originalLevel := globalLogLevel
	originalPrintStackTrace := printStackTrace
	originalTZ, hadTZ := os.LookupEnv("TZ")
	t.Cleanup(func() {
		globalLogLevel = originalLevel
		printStackTrace = originalPrintStackTrace
		if hadTZ {
			t.Setenv("TZ", originalTZ)
		} else {
			t.Setenv("TZ", "")
			if err := os.Unsetenv("TZ"); err != nil {
				t.Fatal(err)
			}
		}
	})
}

func TestLevelFiltering(t *testing.T) {
	preserveGlobalState(t)
	SetLevel(ERROR)

	output := captureStderr(t, func() {
		if entry := Info("not emitted"); entry != "" {
			t.Fatalf("Info() = %q, want empty entry", entry)
		}
		Error("emitted")
	})
	if strings.Contains(output, "not emitted") {
		t.Fatalf("unexpected filtered entry: %q", output)
	}
	if !strings.Contains(output, "ERROR emitted") {
		t.Fatalf("missing error entry: %q", output)
	}
}

func TestEntryFormatting(t *testing.T) {
	preserveGlobalState(t)
	SetLevel(DEBUG)

	output := captureStderr(t, func() {
		Debug("value", 42, "percent %s")
		Infof("row=%d", 7)
	})
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d log lines, want 2: %q", len(lines), output)
	}
	entryPattern := regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} (DEBUG value %!s\(int=42\) percent %s|INFO row=7)$`)
	for _, line := range lines {
		if !entryPattern.MatchString(line) {
			t.Fatalf("unexpected entry shape: %q", line)
		}
	}
}

func TestWarningAndErrorReturnEmittedEntry(t *testing.T) {
	preserveGlobalState(t)
	SetLevel(DEBUG)

	output := captureStderr(t, func() {
		warning := Warning("problem", "one")
		if !strings.HasSuffix(warning.Error(), "WARNING problem one") {
			t.Fatalf("Warning() = %q", warning)
		}
		err := Errorf("problem=%d", 2)
		if !strings.HasSuffix(err.Error(), "ERROR problem=2") {
			t.Fatalf("Errorf() = %q", err)
		}
	})
	if !strings.Contains(output, "WARNING problem one") || !strings.Contains(output, "ERROR problem=2") {
		t.Fatalf("missing emitted entries: %q", output)
	}
}

func TestErroreAndStackTrace(t *testing.T) {
	preserveGlobalState(t)
	SetLevel(DEBUG)

	output := captureStderr(t, func() {
		if Errore(nil) != nil {
			t.Fatal("Errore(nil) returned an error")
		}
		err := errors.New("failed")
		if got := Errore(err); got != err {
			t.Fatalf("Errore() = %v, want original %v", got, err)
		}
		SetPrintStackTrace(true)
		Errore(errors.New("with stack"))
	})
	if !strings.Contains(output, "ERROR failed") || !strings.Contains(output, "ERROR with stack") {
		t.Fatalf("missing error entry: %q", output)
	}
	if !strings.Contains(output, "goroutine ") {
		t.Fatalf("missing stack trace: %q", output)
	}
}

func TestTZHandling(t *testing.T) {
	preserveGlobalState(t)
	SetLevel(DEBUG)
	t.Setenv("TZ", "Pacific/Kiritimati")

	location, err := time.LoadLocation("Pacific/Kiritimati")
	if err != nil {
		t.Fatal(err)
	}
	before := time.Now().In(location).Format(timeFormat)
	validOutput := captureStderr(t, func() {
		Info("valid timezone")
	})
	after := time.Now().In(location).Format(timeFormat)
	validTimestamp := strings.SplitN(validOutput, " INFO ", 2)[0]
	if validTimestamp != before && validTimestamp != after {
		t.Fatalf("valid timezone timestamp = %q, want %q or %q", validTimestamp, before, after)
	}
	t.Setenv("TZ", "not/a/location")
	invalidOutput := captureStderr(t, func() {
		Info("invalid timezone")
	})

	for _, output := range []string{validOutput, invalidOutput} {
		if !regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} INFO `).MatchString(output) {
			t.Fatalf("unexpected timezone entry: %q", output)
		}
	}
}

func TestFatalExits(t *testing.T) {
	if os.Getenv("GO_LOG_FATAL_SUBPROCESS") == "1" {
		Fatal("fatal", "entry")
		return
	}

	command := exec.Command(os.Args[0], "-test.run=^TestFatalExits$")
	command.Env = append(os.Environ(), "GO_LOG_FATAL_SUBPROCESS=1")
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatal("Fatal() subprocess succeeded")
	}
	if exitError, ok := err.(*exec.ExitError); !ok || exitError.ExitCode() != 1 {
		t.Fatalf("Fatal() exit = %v, want status 1", err)
	}
	if !strings.Contains(string(output), "FATAL fatal entry") {
		t.Fatalf("missing fatal entry: %q", output)
	}
}
