package main

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-couchstore"
)

type dbOperation uint8

const (
	db_store_item = dbOperation(iota)
	db_delete_item
	db_compact
)

type dbqitem struct {
	dbname string
	k      string
	data   []byte
	op     dbOperation
}

type dbWriter struct {
	ch   chan dbqitem
	quit chan bool
	db   *couchstore.Couchstore
}

var dbLock = sync.Mutex{}
var dbConns = map[string]*dbWriter{}

func dbPath(n string) string {
	return filepath.Join(*dbRoot, n) + ".couch"
}

func dbBase(n string) string {
	left := 0
	right := len(n)
	if strings.HasPrefix(n, *dbRoot) {
		left = len(*dbRoot)
		if n[left] == '/' {
			left++
		}
	}
	if strings.HasSuffix(n, ".couch") {
		right = len(n) - 6
	}
	return n[left:right]
}

func dbopen(name string) (*couchstore.Couchstore, error) {
	db, err := couchstore.Open(dbPath(name), false)
	return db, err
}

func dbcreate(path string) error {
	db, err := couchstore.Open(path, true)
	if err != nil {
		return err
	}
	db.Close()
	return nil
}

func dbdelete(dbname string) error {
	dbLock.Lock()
	defer dbLock.Unlock()

	writer := dbConns[dbname]
	if writer != nil {
		writer.quit <- true
	}
	delete(dbConns, dbname)

	return os.Remove(dbPath(dbname))
}

func dblist(root string) []string {
	rv := []string{}
	filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if !info.IsDir() && strings.HasSuffix(p, ".couch") {
			rv = append(rv, dbBase(p))
		}
		return err
	})
	return rv
}

func dbWriteLoop(dq *dbWriter) {
	defer dq.db.Close()

	queued := 0
	bulk := dq.db.Bulk()
	defer bulk.Close()
	defer bulk.Commit()

	t := time.NewTimer(*flushTime)

	for {
		select {
		case <-dq.quit:
			return
		case qi := <-dq.ch:
			switch qi.op {
			case db_store_item:
				bulk.Set(couchstore.NewDocInfo(qi.k,
					couchstore.DocIsCompressed),
					couchstore.NewDocument(qi.k,
						string(qi.data)))
			case db_delete_item:
				bulk.Delete(couchstore.NewDocInfo(qi.k, 0))
			default:
				log.Panicf("Unhandled case: %v", qi.op)
			}
			queued++
			if queued > *maxOpQueue {
				log.Printf("Flushing %d items on queue")
				bulk.Commit()
				queued = 0
				t.Stop()
				t = time.NewTimer(*flushTime)
			}
		case <-t.C:
			if queued > 0 {
				log.Printf("Flushing %d items on a timer", queued)
				bulk.Commit()
				queued = 0
			}
			t = time.NewTimer(*flushTime)
		}
	}
}

func dbWriteFun(dbname string) (*dbWriter, error) {
	db, err := dbopen(dbname)
	if err != nil {
		return nil, err
	}

	writer := &dbWriter{
		make(chan dbqitem),
		make(chan bool),
		db,
	}

	go dbWriteLoop(writer)

	return writer, nil
}

func getOrCreateDB(dbname string) (*dbWriter, error) {
	dbLock.Lock()
	defer dbLock.Unlock()

	writer := dbConns[dbname]
	var err error
	if writer == nil {
		writer, err = dbWriteFun(dbname)
		if err != nil {
			return nil, err
		}
		dbConns[dbname] = writer
	}
	return writer, nil
}

func dbstore(dbname string, k string, body []byte) error {
	writer, err := getOrCreateDB(dbname)
	if err != nil {
		return err
	}

	writer.ch <- dbqitem{dbname, k, body, db_store_item}

	return nil
}

func parseKey(s string) int64 {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return -1
	}
	return t.UnixNano()
}
