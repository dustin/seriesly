package main

import (
	"io"
	"log"
	"net"
	"time"

	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/server"
	"github.com/dustin/seriesly/timelib"
)

const (
	CREATE_BUCKET = gomemcached.CommandCode(0x85)
	DELETE_BUCKET = gomemcached.CommandCode(0x86)
	LIST_BUCKETS  = gomemcached.CommandCode(0x87)
	SELECT_BUCKET = gomemcached.CommandCode(0x89)
)

type mcSession struct {
	dbname string
}

func (sess *mcSession) HandleMessage(
	w io.Writer, req *gomemcached.MCRequest) *gomemcached.MCResponse {

	switch req.Opcode {
	case SELECT_BUCKET:
		log.Printf("Selecting bucket %s", req.Key)
		sess.dbname = string(req.Key)
	case gomemcached.SETQ, gomemcached.SET:
		fk := string(req.Key)
		var k string
		if fk == "" {
			k = time.Now().UTC().Format(time.RFC3339Nano)
		} else {
			t, err := timelib.ParseTime(fk)
			if err != nil {
				return &gomemcached.MCResponse{
					Status: gomemcached.EINVAL,
					Body:   []byte("Invalid key"),
				}
			}
			k = t.UTC().Format(time.RFC3339Nano)
		}

		err := dbstore(sess.dbname, k, req.Body)
		if err != nil {
			return &gomemcached.MCResponse{
				Status: gomemcached.NOT_STORED,
				Body:   []byte(err.Error()),
			}
		}

		if req.Opcode == gomemcached.SETQ {
			return nil
		}
	case gomemcached.NOOP:
	default:
		return &gomemcached.MCResponse{Status: gomemcached.UNKNOWN_COMMAND}
	}

	return &gomemcached.MCResponse{}
}

func waitForMCConnections(ls net.Listener) {
	for {
		s, e := ls.Accept()
		if e == nil {
			log.Printf("Got a connection from %s", s.RemoteAddr())
			go memcached.HandleIO(s, &mcSession{})
		} else {
			log.Printf("Error accepting from %s", ls)
		}
	}
}

func listenMC(bindaddr string) net.Listener {
	ls, e := net.Listen("tcp", bindaddr)
	if e != nil {
		log.Fatalf("Error binding to memcached socket:  %s", e)
	}

	log.Printf("Listening for memcached connections on %v", bindaddr)

	go waitForMCConnections(ls)
	return ls
}
