package ioutil2

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestSectionWriter(t *testing.T) {
	f, err := ioutil.TempFile(".", "test_")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		n := f.Name()
		f.Close()
		os.Remove(n)
	}()

	f.Truncate(3)

	rw := NewSectionWriter(f, 0, 1)

	_, err = rw.Write([]byte{'1'})
	if err != nil {
		t.Fatal(err)
	}

	_, err = rw.Write([]byte{'1'})
	if err == nil {
		t.Fatal("must err")
	}

	rw = NewSectionWriter(f, 1, 2)

	_, err = rw.Write([]byte{'2', '3', '4'})
	if err == nil {
		t.Fatal("must err")
	}

	_, err = rw.Write([]byte{'2', '3'})
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 3)
	_, err = f.ReadAt(buf, 0)
	if err != nil {
		t.Fatal(err)
	}

	if string(buf) != "123" {
		t.Fatal(string(buf))
	}
}
