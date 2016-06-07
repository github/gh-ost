/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"bufio"
	"fmt"
	"net"
	"os"

	"github.com/github/gh-ost/go/base"
	"github.com/outbrain/golib/log"
)

type onCommandFunc func(command string, writer *bufio.Writer) error

// Server listens for requests on a socket file or via TCP
type Server struct {
	migrationContext *base.MigrationContext
	unixListener     net.Listener
	tcpListener      net.Listener
	onCommand        onCommandFunc
}

func NewServer(onCommand onCommandFunc) *Server {
	return &Server{
		migrationContext: base.GetMigrationContext(),
		onCommand:        onCommand,
	}
}

func (this *Server) BindSocketFile() (err error) {
	if this.migrationContext.ServeSocketFile == "" {
		return nil
	}
	if base.FileExists(this.migrationContext.ServeSocketFile) {
		os.Remove(this.migrationContext.ServeSocketFile)
	}
	this.unixListener, err = net.Listen("unix", this.migrationContext.ServeSocketFile)
	if err != nil {
		return err
	}
	log.Infof("Listening on unix socket file: %s", this.migrationContext.ServeSocketFile)
	return nil
}

func (this *Server) BindTCPPort() (err error) {
	if this.migrationContext.ServeTCPPort == 0 {
		return nil
	}
	this.tcpListener, err = net.Listen("tcp", fmt.Sprintf(":%d", this.migrationContext.ServeTCPPort))
	if err != nil {
		return err
	}
	log.Infof("Listening on tcp port: %d", this.migrationContext.ServeTCPPort)
	return nil
}

func (this *Server) Serve() (err error) {
	go func() {
		for {
			conn, err := this.unixListener.Accept()
			if err != nil {
				log.Errore(err)
			}
			go this.handleConnection(conn)
		}
	}()
	go func() {
		if this.tcpListener == nil {
			return
		}
		for {
			conn, err := this.tcpListener.Accept()
			if err != nil {
				log.Errore(err)
			}
			go this.handleConnection(conn)
		}
	}()

	return nil
}

func (this *Server) handleConnection(conn net.Conn) (err error) {
	defer conn.Close()
	command, _, err := bufio.NewReader(conn).ReadLine()
	return this.onCommand(string(command), bufio.NewWriter(conn))
}
