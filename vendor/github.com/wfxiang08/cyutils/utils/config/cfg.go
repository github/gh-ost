package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

var (
	fmtErrNotExists      = "no such field: %s"
	fmtErrInvalidCfgFile = "invalid config file: %s"
)

func ErrNotExists(fieldName string) error {
	return fmt.Errorf(fmtErrNotExists, fieldName)
}

func ErrInvalidCfgFile(cfgFile string) error {
	return fmt.Errorf(fmtErrInvalidCfgFile, cfgFile)
}

type Cfg struct {
	l     sync.RWMutex
	fname string
	m     map[string]string
}

func NewCfg(filename string) *Cfg {
	return &Cfg{
		l:     sync.RWMutex{},
		fname: filename,
		m:     make(map[string]string),
	}
}

func (c *Cfg) Load() error {
	c.l.Lock()
	defer c.l.Unlock()
	fp, err := os.Open(c.fname)
	if err != nil {
		return err
	}
	defer fp.Close()
	scanner := bufio.NewScanner(fp)
	for scanner.Scan() {
		trimed := strings.Trim(scanner.Text(), " ")
		if !strings.HasPrefix(trimed, "#") && len(trimed) > 0 {
			parts := strings.SplitN(trimed, "=", 2)
			if len(parts) != 2 {
				return ErrInvalidCfgFile(c.fname)
			}
			c.m[strings.Trim(parts[0], " ")] = strings.Trim(parts[1], " ")
		}
	}
	return nil
}

func (c *Cfg) Save() error {
	file, err := os.OpenFile(c.fname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	for k, v := range c.m {
		fmt.Fprintf(w, "%s=%s\n", k, v)
	}
	return w.Flush()
}

func (c *Cfg) ReadString(k string, def string) (string, error) {
	c.l.RLock()
	defer c.l.RUnlock()
	if v, b := c.m[k]; b {
		return v, nil
	}
	return def, ErrNotExists(k)
}

func (c *Cfg) ReadInt(k string, def int) (int, error) {
	c.l.RLock()
	defer c.l.RUnlock()
	if v, b := c.m[k]; b {
		return strconv.Atoi(v)
	}
	return def, ErrNotExists(k)
}

func (c *Cfg) WriteString(k string, val string) {
	c.l.Lock()
	defer c.l.Unlock()
	c.m[k] = val
}

func (c *Cfg) WriteInt(k string, val int) {
	c.l.Lock()
	defer c.l.Unlock()
	c.m[k] = strconv.Itoa(val)
}
