//// Copyright 2015 Spring Rain Software Compnay LTD. All Rights Reserved.
//// Licensed under the MIT (MIT-LICENSE.txt) license.

package utils

import (
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"
	"unsafe"

	"github.com/wfxiang08/cyutils/utils/config"
	"github.com/wfxiang08/cyutils/utils/errors"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
)

func InitConfigFromFile(filename string) (*config.Cfg, error) {
	ret := config.NewCfg(filename)
	if err := ret.Load(); err != nil {
		return nil, errors.Trace(err)
	}
	return ret, nil
}

//
// 获取带有指定Prefix的Ip
//
func GetIpWithPrefix(prefix string) string {

	ifaces, _ := net.Interfaces()
	// handle err
	for _, i := range ifaces {
		addrs, _ := i.Addrs()
		// handle err
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			ipAddr := ip.String()
			// fmt.Println("ipAddr: ", ipAddr)
			if strings.HasPrefix(ipAddr, prefix) {
				return ipAddr
			}

		}
	}
	return ""
}

func GetExecutorPath() string {
	filedirectory := filepath.Dir(os.Args[0])
	execPath, err := filepath.Abs(filedirectory)
	if err != nil {
		log.PanicErrorf(err, "get executor path failed")
	}
	return execPath
}

type Strings []string

func (s1 Strings) Eq(s2 []string) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i := 0; i < len(s1); i++ {
		if s1[i] != s2[i] {
			return false
		}
	}
	return true
}

const (
	EMPTY_MSG = ""
)

//
// <head> "", tail... ----> head, tail...
// 将msgs拆分成为两部分, 第一部分为: head(包含路由信息);第二部分为: tails包含信息部分
//
func Unwrap(msgs []string) (head string, tails []string) {
	head = msgs[0]
	if len(msgs) > 1 && msgs[1] == EMPTY_MSG {
		tails = msgs[2:]
	} else {
		tails = msgs[1:]
	}
	return
}

// 将msgs中前面多余的EMPTY_MSG删除，可能是 zeromq的不同的socket的配置不匹配导致的
func TrimLeftEmptyMsg(msgs []string) []string {
	for index, msg := range msgs {
		if msg != EMPTY_MSG {
			return msgs[index:len(msgs)]
		}
	}
	return msgs
}

// 打印zeromq中的消息，用于Debug
func PrintZeromqMsgs(msgs []string, prefix string) {

	//	fmt.Printf("Message Length: %d, Prefix: %s\n", len(msgs), prefix)
	//	for idx, msg := range msgs {
	//		fmt.Printf("    idx: %d, msg: %s\n", idx, msg)
	//	}
}

func Copy(s string) string {
	var b []byte
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	h.Data = (*reflect.StringHeader)(unsafe.Pointer(&s)).Data
	h.Len = len(s)
	h.Cap = len(s)

	return string(b)
}

// 判断给定的文件是否存在
func FileExist(file string) bool {
	var err error
	_, err = os.Stat(file)
	return !os.IsNotExist(err)
}

// 获取给定的日期的"开始时刻", 00:00:00
func StartOfDay(t time.Time) time.Time {
	year, month, day := t.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, t.Location())
}

// 第二天的开始时间
func NextStartOfDay(t time.Time) time.Time {
	year, month, day := t.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, t.Location()).AddDate(0, 0, 1)
}
