package main

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/seriesly/timelib"
	"github.com/mschoch/gouchstore"
)

type dbOperation uint8

const (
	opStoreItem = dbOperation(iota)
	opDeleteItem
	opCompact
)

const dbExt = ".couch"

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
	db     *gouchstore.Gouchstore
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

func dbopen(name string) (*gouchstore.Gouchstore, error) {
	path := dbPath(name)
	db, err := gouchstore.Open(dbPath(name), 0)
	if err == nil {
		recordDBConn(path, db)
	}
	return db, err
}

func dbcreate(path string) error {
	db, err := gouchstore.Open(path, gouchstore.OPEN_CREATE)
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

func dbCloseAll() {
	dbLock.Lock()
	defer dbLock.Unlock()

	for n, c := range dbConns {
		log.Printf("Shutting down open conn %v", n)
		c.Close()
	}
}

func dbdelete(dbname string) error {
	dbRemoveConn(dbname)
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

func dbCompact(dq *dbWriter, bulk gouchstore.BulkWriter, queued int,
	qi dbqitem) (gouchstore.BulkWriter, error) {
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
	err := dq.db.Compact(dbn + ".compact")
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

var dbWg = sync.WaitGroup{}

func dbWriteLoop(dq *dbWriter) {
	defer dbWg.Done()

	queued := 0
	bulk := dq.db.Bulk()

	t := time.NewTimer(*flushTime)
	defer t.Stop()
	liveTracker := time.NewTicker(*liveTime)
	defer liveTracker.Stop()
	liveOps := 0

	dbst := dbStats.getOrCreate(dq.dbname)
	defer atomic.StoreUint32(&dbst.qlen, 0)
	defer atomic.AddUint32(&dbst.closes, 1)

	for {
		atomic.StoreUint32(&dbst.qlen, uint32(queued))

		select {
		case <-dq.quit:
			sdt := time.Now()
			bulk.Commit()
			bulk.Close()
			closeDBConn(dq.db)
			dbRemoveConn(dq.dbname)
			log.Printf("Closed %v with %v items in %v",
				dq.dbname, queued, time.Since(sdt))
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
				bulk.Set(gouchstore.NewDocumentInfo(qi.k),
					gouchstore.NewDocument(qi.k, qi.data))
				queued++
			case opDeleteItem:
				queued++
				bulk.Delete(gouchstore.NewDocumentInfo(qi.k))
			case opCompact:
				var err error
				bulk, err = dbCompact(dq, bulk, queued, qi)
				qi.cherr <- err
				atomic.AddUint64(&dbst.written, uint64(queued))
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
				atomic.AddUint64(&dbst.written, uint64(queued))
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
				atomic.AddUint64(&dbst.written, uint64(queued))
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

	dbWg.Add(1)
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

func dbcompact(dbname string) error {
	writer, opened, err := getOrCreateDB(dbname)
	if err != nil {
		return err
	}
	if opened {
		log.Printf("Requesting post-compaction close of %v", dbname)
		defer writer.Close()
	}

	cherr := make(chan error)
	defer close(cherr)
	writer.ch <- dbqitem{dbname: dbname,
		op:    opCompact,
		cherr: cherr,
	}

	return <-cherr
}

func dbGetDoc(dbname, id string) ([]byte, error) {
	db, err := dbopen(dbname)
	if err != nil {
		log.Printf("Error opening db: %v - %v", dbname, err)
		return nil, err
	}
	defer closeDBConn(db)

	doc, err := db.DocumentById(id)
	if err != nil {
		return nil, err
	}
	return doc.Body, err
}

func dbwalk(dbname, from, to string, f func(k string, v []byte) error) error {
	db, err := dbopen(dbname)
	if err != nil {
		log.Printf("Error opening db: %v - %v", dbname, err)
		return err
	}
	defer closeDBConn(db)

	return db.WalkDocs(from, to, func(d *gouchstore.Gouchstore,
		di *gouchstore.DocumentInfo, doc *gouchstore.Document) error {
		return f(di.ID, doc.Body)
	})
}

func dbwalkKeys(dbname, from, to string, f func(k string) error) error {
	db, err := dbopen(dbname)
	if err != nil {
		log.Printf("Error opening db: %v - %v", dbname, err)
		return err
	}
	defer closeDBConn(db)

	return db.AllDocuments(from, to, func(db *gouchstore.Gouchstore, documentInfo *gouchstore.DocumentInfo, userContext interface{}) error {
		return f(documentInfo.ID)
	}, nil)
}

func parseKey(s string) int64 {
	t, err := timelib.ParseCanonicalTime(s)
	if err != nil {
		return -1
	}
	return t.UnixNano()
}
