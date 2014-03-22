package main

import (
	"encoding/json"
	"expvar"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/mschoch/gouchstore"
)

type frameSnap []uintptr

func (f frameSnap) MarshalJSON() ([]byte, error) {
	frames := []string{}
	for _, pc := range f {
		frame := runtime.FuncForPC(pc)
		fn, line := frame.FileLine(frame.Entry())
		frames = append(frames, fmt.Sprintf("%v() - %v:%v",
			frame.Name(), fn, line))
	}
	return json.Marshal(frames)
}

type dbOpenState struct {
	path  string
	funcs frameSnap
}

var openConnLock = sync.Mutex{}
var openConns = map[*gouchstore.Gouchstore]dbOpenState{}

func recordDBConn(path string, db *gouchstore.Gouchstore) {
	callers := make([]uintptr, 32)
	n := runtime.Callers(2, callers)
	openConnLock.Lock()
	openConns[db] = dbOpenState{path, frameSnap(callers[:n-1])}
	openConnLock.Unlock()
}

func closeDBConn(db *gouchstore.Gouchstore) {
	db.Close()
	openConnLock.Lock()
	_, ok := openConns[db]
	delete(openConns, db)
	openConnLock.Unlock()
	if !ok {
		log.Printf("Closing untracked DB: %p", db)
	}
}

func debugListOpenDBs(parts []string, w http.ResponseWriter, req *http.Request) {
	openConnLock.Lock()
	snap := map[string][]frameSnap{}
	for _, st := range openConns {
		snap[st.path] = append(snap[st.path], st.funcs)
	}
	openConnLock.Unlock()

	mustEncode(200, w, snap)
}

type dbStat struct {
	written             uint64
	qlen, opens, closes uint32
}

func (d *dbStat) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{}
	m["written"] = atomic.LoadUint64(&d.written)
	m["qlen"] = atomic.LoadUint32(&d.qlen)
	m["opens"] = atomic.LoadUint32(&d.opens)
	m["closes"] = atomic.LoadUint32(&d.closes)
	return json.Marshal(m)
}

type databaseStats struct {
	m  map[string]*dbStat
	mu sync.Mutex
}

func newQueueMap() *databaseStats {
	return &databaseStats{m: map[string]*dbStat{}}
}

func (q *databaseStats) getOrCreate(name string) *dbStat {
	q.mu.Lock()
	defer q.mu.Unlock()
	rv, ok := q.m[name]
	if !ok {
		rv = &dbStat{}
		q.m[name] = rv
	}
	atomic.AddUint32(&rv.opens, 1)
	return rv
}

func (q *databaseStats) String() string {
	q.mu.Lock()
	defer q.mu.Unlock()
	d, err := json.Marshal(q.m)
	if err != nil {
		log.Fatalf("Error marshaling queueMap: %v", err)
	}
	return string(d)
}

var dbStats = newQueueMap()

func init() {
	expvar.Publish("dbs", dbStats)
}

func debugVars(parts []string, w http.ResponseWriter, req *http.Request) {
	req.URL.Path = strings.Replace(req.URL.Path, "/_debug/vars", "/debug/vars", 1)
	http.DefaultServeMux.ServeHTTP(w, req)
}
