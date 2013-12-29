package main

import (
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cznic/kv"
)

type dbOperation uint8

const (
	opStoreItem = dbOperation(iota)
	opDeleteItem
)

const dbExt = ".kv"

type dbqitem struct {
	dbname string
	k      string
	data   []byte
	op     dbOperation
	cherr  chan error
}

type dbWriter struct {
	dbname string
	ch     chan dbqitem
	quit   chan bool
	db     *kv.DB
}

var errClosed = errors.New("closed")

func (w *dbWriter) Close() error {
	select {
	case <-w.quit:
		return errClosed
	default:
	}
	close(w.quit)
	return nil
}

var dbLock = sync.Mutex{}
var dbConns = map[string]*dbWriter{}

func dbPath(n string) string {
	return filepath.Join(*dbRoot, n) + dbExt
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
	if strings.HasSuffix(n, dbExt) {
		right = len(n) - len(dbExt)
	}
	return n[left:right]
}

func dbopen(name string) (*kv.DB, error) {
	path := dbPath(name)
	db, err := kv.Open(dbPath(name), &kv.Options{})
	if err == nil {
		recordDBConn(path, db)
	}
	return db, err
}

func dbcreate(path string) error {
	db, err := kv.Create(path, &kv.Options{})
	if err != nil {
		return err
	}
	recordDBConn(path, db)
	closeDBConn(db)
	return nil
}

func dbRemoveConn(dbname string) {
	dbLock.Lock()
	defer dbLock.Unlock()

	writer := dbConns[dbname]
	if writer != nil && writer.quit != nil {
		writer.Close()
	}
	delete(dbConns, dbname)
}

func dbdelete(dbname string) error {
	return os.Remove(dbPath(dbname))
}

func dblist(root string) []string {
	rv := []string{}
	filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err == nil {
			if !info.IsDir() && strings.HasSuffix(p, dbExt) {
				rv = append(rv, dbBase(p))
			}
		} else {
			log.Printf("Error on %#v: %v", p, err)
		}
		return nil
	})
	return rv
}

func dbWriteLoop(dq *dbWriter) {
	queued := 0

	t := time.NewTimer(*flushTime)
	defer t.Stop()
	liveTracker := time.NewTicker(*liveTime)
	defer liveTracker.Stop()
	liveOps := 0

	if err := dq.db.BeginTransaction(); err != nil {
		log.Fatalf("Error beginning transaction on %v: %v", dq.dbname, err)
	}

	for {
		select {
		case <-dq.quit:
			if err := dq.db.Commit(); err != nil {
				log.Printf("Error committing %v: %v", dq.dbname, err)
			}
			closeDBConn(dq.db)
			dbRemoveConn(dq.dbname)
			log.Printf("Closed %v", dq.dbname)
			return
		case <-liveTracker.C:
			if queued == 0 && liveOps == 0 {
				log.Printf("Closing idle DB: %v", dq.dbname)
				close(dq.quit)
			}
			liveOps = 0
		case qi := <-dq.ch:
			liveOps++
			switch qi.op {
			case opStoreItem:
				if err := dq.db.Set([]byte(qi.k), qi.data); err != nil {
					log.Printf("Error queueing %v: %v", qi, err)
				}
				queued++
			case opDeleteItem:
				dq.db.Delete([]byte(qi.k))
				queued++
			default:
				log.Panicf("Unhandled case: %v", qi.op)
			}
			if queued >= *maxOpQueue {
				start := time.Now()
				if err := dq.db.Commit(); err != nil {
					log.Printf("Error committing: %v", err)
				}
				if *verbose {
					log.Printf("Flush of %d items took %v",
						queued, time.Since(start))
				}
				if err := dq.db.BeginTransaction(); err != nil {
					log.Printf("Error beginning new transaction: %v", err)
				}
				queued = 0
				t.Reset(*flushTime)
			}
		case <-t.C:
			if queued > 0 {
				start := time.Now()
				if err := dq.db.Commit(); err != nil {
					log.Printf("Error committing: %v", err)
				}
				if *verbose {
					log.Printf("Flush of %d items from timer took %v",
						queued, time.Since(start))
				}
				if err := dq.db.BeginTransaction(); err != nil {
					log.Printf("Error beginning new transaction: %v", err)
				}
				queued = 0
			}
			t.Reset(*flushTime)
		}
	}
}

func dbWriteFun(dbname string) (*dbWriter, error) {
	db, err := dbopen(dbname)
	if err != nil {
		return nil, err
	}

	writer := &dbWriter{
		dbname,
		make(chan dbqitem, *maxOpQueue),
		make(chan bool),
		db,
	}

	go dbWriteLoop(writer)

	return writer, nil
}

func getOrCreateDB(dbname string) (*dbWriter, bool, error) {
	dbLock.Lock()
	defer dbLock.Unlock()

	writer := dbConns[dbname]
	var err error
	opened := false
	if writer == nil {
		writer, err = dbWriteFun(dbname)
		if err != nil {
			return nil, false, err
		}
		dbConns[dbname] = writer
		opened = true
	}
	return writer, opened, nil
}

func dbstore(dbname string, k string, body []byte) error {
	writer, _, err := getOrCreateDB(dbname)
	if err != nil {
		return err
	}

	writer.ch <- dbqitem{dbname, k, body, opStoreItem, nil}

	return nil
}

func dbGetDoc(dbname, id string) ([]byte, error) {
	db, err := dbopen(dbname)
	if err != nil {
		log.Printf("Error opening db: %v - %v", dbname, err)
		return nil, err
	}
	defer closeDBConn(db)

	doc, err := db.Get(nil, []byte(id))
	if err != nil {
		return nil, err
	}
	return doc, err
}

func dbwalk(dbname, from, to string, f func(k, v []byte) error) error {
	db, err := dbopen(dbname)
	if err != nil {
		log.Printf("Error opening db: %v - %v", dbname, err)
		return err
	}
	defer closeDBConn(db)

	e, _, err := db.Seek([]byte(from))
	if err != nil {
		return err
	}

	for {
		key, value, err := e.Next()
		switch err {
		case nil:
		case io.EOF:
			return nil
		default:
			return err
		}

		err = f(key, value)
		if err != nil {
			return err
		}
	}
}

func dbwalkKeys(dbname, from, to string, f func(string) error) error {
	return dbwalk(dbname, from, to, func(k, v []byte) error {
		return f(string(k))
	})
}

func parseKey(s string) int64 {
	t, err := parseCanonicalTime(s)
	if err != nil {
		return -1
	}
	return t.UnixNano()
}
