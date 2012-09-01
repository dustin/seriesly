package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/dustin/go-couchstore"
	"github.com/dustin/go-humanize"
)

func serverInfo(parts []string, w http.ResponseWriter, req *http.Request) {
	sinfo := map[string]string{
		"seriesly": "Why so series?", "version": "seriesly 0.0",
	}
	mustEncode(200, w, sinfo)
}

func listDatabases(parts []string, w http.ResponseWriter, req *http.Request) {
	mustEncode(200, w, dblist(*dbRoot))
}

func notImplemented(parts []string, w http.ResponseWriter, req *http.Request) {
	emitError(501, w, "not implemented", "TODO")
}

func createDB(parts []string, w http.ResponseWriter, req *http.Request) {
	path := dbPath(parts[0])
	err := dbcreate(path)
	if err == nil {
		w.WriteHeader(201)
	} else {
		emitError(500, w, "Server Error", err.Error())
	}
}

func checkDB(args []string, w http.ResponseWriter, req *http.Request) {
	dbname := args[0]
	if db, err := dbopen(dbname); err == nil {
		db.Close()
		w.WriteHeader(200)
	} else {
		w.WriteHeader(404)
	}
}

func newDocument(args []string, w http.ResponseWriter, req *http.Request) {
	var k, fk string
	form, err := url.ParseQuery(req.URL.RawQuery)
	if err == nil {
		fk = form.Get("ts")
	}

	if fk == "" {
		k = time.Now().UTC().Format(time.RFC3339Nano)
	} else {
		t, err := parseTime(fk)
		if err != nil {
			emitError(400, w, "Bad time format", err.Error())
			return
		}
		k = t.UTC().Format(time.RFC3339Nano)
	}
	putDocument([]string{args[0], k}, w, req)
}

func putDocument(args []string, w http.ResponseWriter, req *http.Request) {
	dbname := args[0]
	k := args[1]
	defer req.Body.Close()
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		emitError(400, w, "Bad Request",
			fmt.Sprintf("Error reading body: %v", err))
		return
	}

	err = dbstore(dbname, k, body)

	if err == nil {
		w.WriteHeader(201)
	} else {
		emitError(500, w, "Error storing data", err.Error())
	}
}

func cleanupRangeParam(in, def string) (string, error) {
	if in == "" {
		return def, nil
	}
	t, err := parseTime(in)
	if err != nil {
		return in, err
	}
	return t.UTC().Format(time.RFC3339Nano), nil
}

func query(args []string, w http.ResponseWriter, req *http.Request) {
	// Parse the params

	req.ParseForm()

	group, err := strconv.Atoi(req.FormValue("group"))
	if err != nil {
		emitError(400, w, "Bad group value", err.Error())
		return
	}

	from, err := cleanupRangeParam(req.FormValue("from"), "")
	if err != nil {
		emitError(400, w, "Bad from value: %v", err.Error())
		return
	}
	to, err := cleanupRangeParam(req.FormValue("to"), "")
	if err != nil {
		emitError(400, w, "Bad to value: %v", err.Error())
		return
	}

	ptrs := req.Form["ptr"]
	reds := make([]Reducer, 0, len(ptrs))
	for _, r := range req.Form["reducer"] {
		f, ok := reducers[r]
		if !ok {
			emitError(400, w, "No such reducer", r)
			return
		}
		reds = append(reds, f)
	}
	if len(ptrs) != len(reds) {
		emitError(400, w, "Parameter mismatch",
			"Must supply the same number of pointers and reducers")
		return
	}

	// Open the DB and do the work.

	db, err := dbopen(args[0])
	if err != nil {
		emitError(500, w, "Error opening DB", err.Error())
		return
	}
	defer db.Close()

	chunk := int64(time.Duration(group) * time.Millisecond)

	infos := []*couchstore.DocInfo{}
	prevg := int64(0)

	ch := make(chan processOut, *queryWorkers)
	cherr := make(chan error)
	defer close(ch)
	defer close(cherr)
	var started, finished int32
	var totalKeys int

	start := time.Now()
	before := start.Add(*queryTimeout)

	go func() {
		err := db.Walk(from, func(d *couchstore.Couchstore,
			di *couchstore.DocInfo) error {
			if to != "" && di.ID() >= to {
				return couchstore.StopIteration
			}

			k := parseKey(di.ID())
			g := (k / chunk) * chunk
			totalKeys++

			if g != prevg && len(infos) > 0 {
				atomic.AddInt32(&started, 1)
				processorInput <- processIn{args[0], prevg, infos, ptrs, reds,
					before, ch}

				infos = infos[:0]
			}
			infos = append(infos, di)
			prevg = g

			return nil
		})

		if err == nil && len(infos) > 0 {
			atomic.AddInt32(&started, 1)
			processorInput <- processIn{args[0], prevg, infos, ptrs, reds,
				before, ch}
		}

		cherr <- err
	}()

	output := map[string]interface{}{}
	going := true

	if err == nil {
		for going || (started-finished) > 0 {
			select {
			case po := <-ch:
				atomic.AddInt32(&finished, 1)
				if po.err != nil {
					err = po.err
				}
				output[strconv.FormatInt(po.key/1e6, 10)] = po.value
			case err = <-cherr:
				going = false
			}
		}
	}

	log.Printf("Completed query processing in %v, %v keys, %v chunks",
		time.Since(start), humanize.Comma(int64(totalKeys)),
		humanize.Comma(int64(started)))

	if err != nil {
		log.Printf("Error processing query: %v", err)
		emitError(500, w, "Error traversing DB", err.Error())
	} else {
		e := json.NewEncoder(w)
		err := e.Encode(output)
		if err != nil {
			emitError(500, w, "Error encoding output", err.Error())
		}
	}
}

func deleteDB(parts []string, w http.ResponseWriter, req *http.Request) {
	err := dbdelete(parts[0])
	if err == nil {
		mustEncode(200, w, map[string]interface{}{"ok": true})
	} else {
		emitError(500, w, "Error deleting DB", err.Error())
	}
}

// TODO:

func dbInfo(parts []string, w http.ResponseWriter, req *http.Request) {
	notImplemented(parts, w, req)
}

func dbChanges(parts []string, w http.ResponseWriter, req *http.Request) {
	notImplemented(parts, w, req)
}

func getDocument(parts []string, w http.ResponseWriter, req *http.Request) {
	notImplemented(parts, w, req)
}

func rmDocument(parts []string, w http.ResponseWriter, req *http.Request) {
	notImplemented(parts, w, req)
}
