package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/dustin/gojson"
	"github.com/dustin/seriesly/timelib"
	"github.com/mschoch/gouchstore"
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
		closeDBConn(db)
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
		t, err := timelib.ParseTime(fk)
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

	err = json.Validate(body)
	if err != nil {
		emitError(400, w, "Error parsing JSON data", err.Error())
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
	t, err := timelib.ParseTime(in)
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
		emitError(400, w, "Bad from value", err.Error())
		return
	}
	to, err := cleanupRangeParam(req.FormValue("to"), "")
	if err != nil {
		emitError(400, w, "Bad to value", err.Error())
		return
	}

	ptrs := req.Form["ptr"]
	reds := make([]string, 0, len(ptrs))
	for _, r := range req.Form["reducer"] {
		_, ok := reducers[r]
		if !ok {
			emitError(400, w, "No such reducer", r)
			return
		}
		reds = append(reds, r)
	}

	if len(ptrs) < 1 {
		emitError(400, w, "Pointer required",
			"At least one ptr argument is required")
		return
	}

	if len(ptrs) != len(reds) {
		emitError(400, w, "Parameter mismatch",
			"Must supply the same number of pointers and reducers")
		return
	}

	filters := req.Form["f"]
	filtervals := req.Form["fv"]
	if len(filters) != len(filtervals) {
		emitError(400, w, "Parameter mismatch",
			"Must supply the same number of filters and filter values")
		return
	}

	q := executeQuery(args[0], from, to, group, ptrs, reds, filters, filtervals)
	defer close(q.out)
	defer close(q.cherr)

	output := io.Writer(newGzippingWriter(w, req))
	defer output.(io.Closer).Close()

	going := true
	finished := int32(0)
	started := false
	walkComplete := false
	for going {
		select {
		case po := <-q.out:
			if !started {
				started = true
				w.WriteHeader(200)
				output.Write([]byte{'{'})
			}
			if finished != 0 {
				output.Write([]byte{',', '\n'})
			}
			finished++

			_, err := fmt.Fprintf(output, `"%d": `, po.key/1e6)
			if err == nil {
				var d []byte
				d, err = json.Marshal(po.value)
				if err == nil {
					_, err = output.Write(d)
				}
			}
			if err != nil {
				log.Printf("Error sending item: %v", err)
				output = ioutil.Discard
				q.before = time.Time{}
			}
		case err = <-q.cherr:
			if err != nil {
				if !started {
					w.WriteHeader(500)
					w.Header().Set("Content-Type", "text/plain")
					fmt.Fprintf(output, "Error beginning traversal: %v", err)
				}
				log.Printf("Walk completed with err: %v", err)
				going = false
			}
			walkComplete = true
		}
		going = (q.started-finished > 0) || !walkComplete
	}

	if started {
		output.Write([]byte{'}'})
	}

	duration := time.Since(q.start)
	if duration > *minQueryLogDuration {
		log.Printf("Completed query processing in %v, %v keys, %v chunks",
			duration, humanize.Comma(int64(q.totalKeys)),
			humanize.Comma(int64(q.started)))
	}
}

func deleteBulk(args []string, w http.ResponseWriter, req *http.Request) {
	// Parse the params

	req.ParseForm()

	compactAfter := strings.ToLower(req.FormValue("compact"))

	from, err := cleanupRangeParam(req.FormValue("from"), "")
	if err != nil {
		emitError(400, w, "Bad from value", err.Error())
		return
	}
	to, err := cleanupRangeParam(req.FormValue("to"), "")
	if err != nil {
		emitError(400, w, "Bad to value", err.Error())
		return
	}

	db, err := dbopen(args[0])
	if err != nil {
		emitError(400, w, "Unable to open Database", err.Error())
		return
	}

	bulk := db.Bulk()
	deleteCount := 0
	commitThreshold := 10000

	err = dbwalkKeys(args[0], from, to, func(k string) error {

		bulk.Delete(gouchstore.NewDocumentInfo(k))
		deleteCount++
		if deleteCount >= commitThreshold {
			bulk.Commit()
			deleteCount = 0
		}
		return err
	})

	if deleteCount > 0 {
		bulk.Commit()
	}

	bulk.Close()

	if compactAfter == "true" {
		err = dbcompact(args[0])
	}

	w.WriteHeader(201)

}
func deleteDB(parts []string, w http.ResponseWriter, req *http.Request) {
	err := dbdelete(parts[0])
	if err == nil {
		mustEncode(200, w, map[string]interface{}{"ok": true})
	} else {
		emitError(500, w, "Error deleting DB", err.Error())
	}
}

func compact(parts []string, w http.ResponseWriter, req *http.Request) {
	err := dbcompact(parts[0])
	if err == nil {
		mustEncode(200, w, map[string]interface{}{"ok": true})
	} else {
		emitError(500, w, "Error compacting DB", err.Error())
	}
}

func canGzip(req *http.Request) bool {
	acceptable := req.Header.Get("accept-encoding")
	return strings.Contains(acceptable, "gzip")
}

type gzippingWriter struct {
	gz     *gzip.Writer
	output io.Writer
}

func newGzippingWriter(w http.ResponseWriter, req *http.Request) *gzippingWriter {
	rv := &gzippingWriter{output: w}
	if canGzip(req) {
		w.Header().Set("Content-Encoding", "gzip")
		rv.gz = gzip.NewWriter(w)
		rv.output = rv.gz
	}
	return rv
}

func (g *gzippingWriter) Write(b []byte) (int, error) {
	return g.output.Write(b)
}

func (g *gzippingWriter) Close() error {
	if g.gz != nil {
		return g.gz.Close()
	}
	return nil
}

func allDocs(args []string, w http.ResponseWriter, req *http.Request) {
	// Parse the params

	req.ParseForm()

	from, err := cleanupRangeParam(req.FormValue("from"), "")
	if err != nil {
		emitError(400, w, "Bad from value", err.Error())
		return
	}
	to, err := cleanupRangeParam(req.FormValue("to"), "")
	if err != nil {
		emitError(400, w, "Bad to value", err.Error())
		return
	}

	limit, err := strconv.Atoi(req.FormValue("limit"))
	if err != nil {
		limit = 2000000000
	}

	output := newGzippingWriter(w, req)
	defer output.Close()
	w.WriteHeader(200)

	output.Write([]byte{'{'})
	defer output.Write([]byte{'}'})

	seenOne := false

	walked := 0
	err = dbwalk(args[0], from, to, func(k string, v []byte) error {
		if walked > limit {
			return io.EOF
		}
		walked++
		if seenOne {
			output.Write([]byte(",\n"))
		} else {
			seenOne = true
		}
		_, err := fmt.Fprintf(output, `"%s": `, k)
		if err != nil {
			return err
		}
		_, err = output.Write(v)
		return err
	})
}

func dumpDocs(args []string, w http.ResponseWriter, req *http.Request) {
	// Parse the params

	req.ParseForm()

	from, err := cleanupRangeParam(req.FormValue("from"), "")
	if err != nil {
		emitError(400, w, "Bad from value", err.Error())
		return
	}
	to, err := cleanupRangeParam(req.FormValue("to"), "")
	if err != nil {
		emitError(400, w, "Bad to value", err.Error())
		return
	}

	limit, err := strconv.Atoi(req.FormValue("limit"))
	if err != nil {
		limit = 2000000000
	}

	output := newGzippingWriter(w, req)
	defer output.Close()
	w.WriteHeader(200)

	walked := 0
	err = dbwalk(args[0], from, to, func(k string, v []byte) error {
		if walked > limit {
			return io.EOF
		}
		walked++
		_, err := fmt.Fprintf(output, `{"%s": `, k)
		if err != nil {
			return err
		}
		_, err = output.Write(v)
		output.Write([]byte{'}', '\n'})
		return err
	})
}

func getDocument(parts []string, w http.ResponseWriter, req *http.Request) {
	d, err := dbGetDoc(parts[0], parts[1])
	if err == nil {
		w.Write(d)
	} else {
		emitError(404, w, "Error retrieving value", err.Error())
	}
}

func dbInfo(args []string, w http.ResponseWriter, req *http.Request) {
	db, err := dbopen(args[0])
	if err != nil {
		emitError(500, w, "Error opening DB", err.Error())
		return
	}
	defer closeDBConn(db)

	inf, err := db.DatabaseInfo()
	if err == nil {
		mustEncode(200, w, map[string]interface{}{
			"last_seq":      inf.LastSeq,
			"doc_count":     inf.DocumentCount,
			"deleted_count": inf.DeletedCount,
			"space_used":    inf.SpaceUsed,
			"header_pos":    inf.HeaderPosition,
		})
	} else {
		emitError(500, w, "Error getting db info", err.Error())
	}
}

// TODO:

func dbChanges(parts []string, w http.ResponseWriter, req *http.Request) {
	notImplemented(parts, w, req)
}

func rmDocument(parts []string, w http.ResponseWriter, req *http.Request) {
	notImplemented(parts, w, req)
}
