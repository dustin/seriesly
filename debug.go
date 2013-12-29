package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"sync"

	"github.com/cznic/kv"
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
var openConns = map[*kv.DB]dbOpenState{}

func recordDBConn(path string, db *kv.DB) {
	callers := make([]uintptr, 32)
	n := runtime.Callers(2, callers)
	openConnLock.Lock()
	openConns[db] = dbOpenState{path, frameSnap(callers[:n-1])}
	openConnLock.Unlock()
}

func closeDBConn(db *kv.DB) {
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
