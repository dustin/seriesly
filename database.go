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
	cherr  chan error
}

type dbWriter struct {
	dbname string
	ch     chan dbqitem
	quit   chan bool
	db     *couchstore.Couchstore
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
	path := dbPath(name)
	db, err := couchstore.Open(dbPath(name), false)
	if err == nil {
		recordDBConn(path, db)
	}
	return db, err
}

func dbcreate(path string) error {
	db, err := couchstore.Open(path, true)
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
		close(writer.quit)
		writer.quit = nil
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
			if !info.IsDir() && strings.HasSuffix(p, ".couch") {
				rv = append(rv, dbBase(p))
			}
		} else {
			log.Printf("Error on %#v: %v", p, err)
		}
		return nil
	})
	return rv
}

func dbCompact(dq *dbWriter, bulk couchstore.BulkWriter, queued int,
	qi dbqitem) (couchstore.BulkWriter, error) {
	start := time.Now()
	if queued > 0 {
		bulk.Commit()
		if *verbose {
			log.Printf("Flushed %d items in %v for pre-compact",
				queued, time.Since(start))
		}
		bulk.Close()
	}
	dbn := dbPath(dq.dbname)
	queued = 0
	start = time.Now()
	err := dq.db.CompactTo(dbn + ".compact")
	if err != nil {
		log.Printf("Error compacting: %v", err)
		return dq.db.Bulk(), err
	}
	log.Printf("Finished compaction of %v in %v", dq.dbname,
		time.Since(start))
	err = os.Rename(dbn+".compact", dbn)
	if err != nil {
		log.Printf("Error putting compacted data back")
		return dq.db.Bulk(), err
	}

	log.Printf("Reopening post-compact")
	closeDBConn(dq.db)

	dq.db, err = dbopen(dq.dbname)
	if err != nil {
		log.Fatalf("Error reopening DB after compaction: %v", err)
	}
	return dq.db.Bulk(), nil
}

func dbWriteLoop(dq *dbWriter) {
	queued := 0
	bulk := dq.db.Bulk()

	t := time.NewTimer(*flushTime)
	defer t.Stop()
	liveTracker := time.NewTicker(*liveTime)
	defer liveTracker.Stop()
	liveOps := 0

	for {
		select {
		case <-dq.quit:
			bulk.Close()
			bulk.Commit()
			closeDBConn(dq.db)
			return
		case <-liveTracker.C:
			if queued == 0 && liveOps == 0 {
				log.Printf("Closing idle DB: %v", dq.dbname)
				closeDBConn(dq.db)
				dbRemoveConn(dq.dbname)
				return
			}
			liveOps = 0
		case qi := <-dq.ch:
			liveOps++
			switch qi.op {
			case db_store_item:
				bulk.Set(couchstore.NewDocInfo(qi.k,
					couchstore.DocIsCompressed),
					couchstore.NewDocument(qi.k, qi.data))
				queued++
			case db_delete_item:
				queued++
				bulk.Delete(couchstore.NewDocInfo(qi.k, 0))
			case db_compact:
				var err error
				bulk, err = dbCompact(dq, bulk, queued, qi)
				qi.cherr <- err
				queued = 0
			default:
				log.Panicf("Unhandled case: %v", qi.op)
			}
			if queued >= *maxOpQueue {
				start := time.Now()
				bulk.Commit()
				if *verbose {
					log.Printf("Flush of %d items took %v",
						queued, time.Since(start))
				}
				queued = 0
				t.Reset(*flushTime)
			}
		case <-t.C:
			if queued > 0 {
				start := time.Now()
				bulk.Commit()
				if *verbose {
					log.Printf("Flush of %d items from timer took %v",
						queued, time.Since(start))
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

	writer.ch <- dbqitem{dbname, k, body, db_store_item, nil}

	return nil
}

func dbcompact(dbname string) error {
	writer, err := getOrCreateDB(dbname)
	if err != nil {
		return err
	}

	cherr := make(chan error)
	defer close(cherr)
	writer.ch <- dbqitem{dbname: dbname,
		op:    db_compact,
		cherr: cherr,
	}

	return <-cherr
}

func dbGetDoc(dbname, id string) ([]byte, error) {
	db, err := dbopen(dbname)
	if err != nil {
		log.Printf("Error opening db: %v - %v", dbname, err)
		return []byte{}, err
	}
	defer closeDBConn(db)

	doc, _, err := db.Get(id)
	if err != nil {
		return []byte{}, err
	}
	return doc.Value(), err
}

func dbwalk(dbname, from, to string, f func(k string, v []byte) error) error {
	db, err := dbopen(dbname)
	if err != nil {
		log.Printf("Error opening db: %v - %v", dbname, err)
		return err
	}
	defer closeDBConn(db)

	return db.WalkDocs(from, func(d *couchstore.Couchstore,
		di *couchstore.DocInfo, doc *couchstore.Document) error {
		if to != "" && di.ID() >= to {
			return couchstore.StopIteration
		}

		return f(di.ID(), doc.Value())
	})
}

func dbwalkKeys(dbname, from, to string, f func(k string) error) error {
	db, err := dbopen(dbname)
	if err != nil {
		log.Printf("Error opening db: %v - %v", dbname, err)
		return err
	}
	defer closeDBConn(db)

	return db.Walk(from, func(d *couchstore.Couchstore,
		di *couchstore.DocInfo) error {
		if to != "" && di.ID() >= to {
			return couchstore.StopIteration
		}

		return f(di.ID())
	})
}

func parseKey(s string) int64 {
	t, err := parseCanonicalTime(s)
	if err != nil {
		return -1
	}
	return t.UnixNano()
}
