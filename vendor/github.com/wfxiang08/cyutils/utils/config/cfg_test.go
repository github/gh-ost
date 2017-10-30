package config

import (
	"testing"
)

func TestCfg(t *testing.T) {
	c := NewCfg("test.ini")
	if err := c.Load() ; err != nil {
		t.Error(err)
	}
	c.WriteInt("hello", 42)
	c.WriteString("hello1", "World")

	v, err := c.ReadInt("hello", 0)
	if err != nil || v != 42 {
		t.Error(err)
	}

	v1, err := c.ReadString("hello1", "")
	if err != nil || v1 != "World" {
		t.Error(err)
	}

	if err := c.Save(); err != nil {
		t.Error(err)
	}
}
