package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/dustin/go-couchstore"
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
	k := time.Now().UTC().Format(time.RFC3339Nano)
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

func query(args []string, w http.ResponseWriter, req *http.Request) {
	// Parse the params

	req.ParseForm()

	group, err := strconv.Atoi(req.FormValue("group"))
	if err != nil {
		emitError(400, w, "Bad group value", err.Error())
		return
	}

	from := req.FormValue("from")
	to := req.FormValue("to")

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
	}
	defer db.Close()

	chunk := int64(time.Duration(group) * time.Second)

	collection := make([][]*string, len(ptrs))
	prevg := int64(0)

	output := map[string]interface{}{}

	err = db.WalkDocs(from, func(d *couchstore.Couchstore,
		di couchstore.DocInfo, doc couchstore.Document) error {
		if to != "" && di.ID() >= to {
			return couchstore.StopIteration
		}

		k := parseKey(di.ID())
		g := (k / chunk) * chunk

		if g != prevg && len(collection[0]) > 0 {
			log.Printf("Emitting at %v with %v items!",
				prevg, len(collection[0]))

			reduced := reduce(collection, reds)
			log.Printf("Reduced: %v", reduced)
			output[strconv.FormatInt(prevg/1e9, 10)] = reduced

			collection = make([][]*string, len(ptrs))
		}
		prevg = g

		processDoc(collection, doc.Value(), ptrs)

		return nil
	})
	if err != nil {
		emitError(500, w, "Error traversing DB", err.Error())
	} else {
		if len(collection[0]) > 0 {
			reduced := reduce(collection, reds)
			log.Printf("Reduced: %v", reduced)
			output[strconv.FormatInt(prevg/1e9, 10)] = reduced
		}

		e := json.NewEncoder(w)
		err := e.Encode(output)
		if err != nil {
			emitError(500, w, "Error encoding output", err.Error())
		}
	}
}

// TODO:

func dbInfo(parts []string, w http.ResponseWriter, req *http.Request) {
	notImplemented(parts, w, req)
}

func dbChanges(parts []string, w http.ResponseWriter, req *http.Request) {
	notImplemented(parts, w, req)
}

func deleteDB(parts []string, w http.ResponseWriter, req *http.Request) {
	notImplemented(parts, w, req)
}

func getDocument(parts []string, w http.ResponseWriter, req *http.Request) {
	notImplemented(parts, w, req)
}

func rmDocument(parts []string, w http.ResponseWriter, req *http.Request) {
	notImplemented(parts, w, req)
}
