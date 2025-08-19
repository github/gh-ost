package gspt

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"encoding/binary"
	"encoding/hex"

	"crypto/md5"
	"os/exec"
)

func randomMD5() string {
	str := md5.New()
	random := new(bytes.Buffer)

	binary.Write(random, binary.LittleEndian, time.Now().UTC().UnixNano())

	str.Write(random.Bytes())

	return hex.EncodeToString(str.Sum(nil))
}

func TestSetProcTitle(t *testing.T) {
	if HaveSetProcTitle == HaveNone {
		t.SkipNow()
	}

	title := randomMD5()

	SetProcTitle(title)

	out, err := exec.Command("/bin/ps", "ax").Output()
	if err != nil {
		// No ps available on this platform.
		t.SkipNow()
	} else if !strings.Contains(string(out), title) {
		t.FailNow()
	}
}

func TestSetProcTitleFast(t *testing.T) {
	if HaveSetProcTitleFast == HaveNone {
		t.SkipNow()
	}

	title := randomMD5()

	SetProcTitleFast(title)

	out, err := exec.Command("/bin/ps", "ax").Output()
	if err != nil {
		// No ps available on this platform.
		t.SkipNow()
	} else if !strings.Contains(string(out), title) {
		t.FailNow()
	}
}
