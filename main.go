package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"runtime"
	"time"
)

var dbRoot = flag.String("root", "db", "Root directory for database files.")
var flushTime = flag.Duration("flushDelay", time.Second*5,
	"Maximum amount of time to wait before flushing")
var maxOpQueue = flag.Int("maxOpQueue", 1000,
	"Maximum number of queued items before flushing")
var staticPath = flag.String("static", "static", "Path to static data")
var queryTimeout = flag.Duration("maxQueryTime", time.Minute*5,
	"Maximum amount of time a query is allowed to process.")
var queryBacklog = flag.Int("queryBacklog", 0, "Query scan/group backlog size")
var docBacklog = flag.Int("docBacklog", 0, "MR group request backlog size")

type routeHandler func(parts []string, w http.ResponseWriter, req *http.Request)

type routingEntry struct {
	Method   string
	Path     *regexp.Regexp
	Handler  routeHandler
	Deadline time.Duration
}

const dbMatch = "[-%+()$_a-zA-Z0-9]+"

var defaultDeadline = time.Millisecond * 50

var routingTable []routingEntry = []routingEntry{
	routingEntry{"GET", regexp.MustCompile("^/$"),
		serverInfo, defaultDeadline},
	routingEntry{"GET", regexp.MustCompile("^/_static/(.*)"),
		staticHandler, defaultDeadline},
	// Database stuff
	routingEntry{"GET", regexp.MustCompile("^/_all_dbs$"),
		listDatabases, defaultDeadline},
	routingEntry{"GET", regexp.MustCompile("^/_(.*)"),
		reservedHandler, defaultDeadline},
	routingEntry{"GET", regexp.MustCompile("^/(" + dbMatch + ")/?$"),
		dbInfo, defaultDeadline},
	routingEntry{"HEAD", regexp.MustCompile("^/(" + dbMatch + ")/?$"),
		checkDB, defaultDeadline},
	routingEntry{"GET", regexp.MustCompile("^/(" + dbMatch + ")/_changes$"),
		dbChanges, defaultDeadline},
	routingEntry{"GET", regexp.MustCompile("^/(" + dbMatch + ")/_query$"),
		query, *queryTimeout},
	routingEntry{"POST", regexp.MustCompile("^/(" + dbMatch + ")/_compact"),
		compact, time.Second * 30},
	routingEntry{"PUT", regexp.MustCompile("^/(" + dbMatch + ")/?$"),
		createDB, defaultDeadline},
	routingEntry{"DELETE", regexp.MustCompile("^/(" + dbMatch + ")/?$"),
		deleteDB, defaultDeadline},
	routingEntry{"POST", regexp.MustCompile("^/(" + dbMatch + ")/?$"),
		newDocument, defaultDeadline},
	// Document stuff
	routingEntry{"PUT", regexp.MustCompile("^/(" + dbMatch + ")/([^/]+)$"),
		putDocument, defaultDeadline},
	routingEntry{"GET", regexp.MustCompile("^/(" + dbMatch + ")/([^/]+)$"),
		getDocument, defaultDeadline},
	routingEntry{"DELETE", regexp.MustCompile("^/(" + dbMatch + ")/([^/]+)$"),
		rmDocument, defaultDeadline},
}

func mustEncode(status int, w http.ResponseWriter, ob interface{}) {
	b, err := json.Marshal(ob)
	if err != nil {
		log.Fatalf("Error encoding %v.", ob)
	}
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(b)))
	w.WriteHeader(status)
	w.Write(b)
}

func emitError(status int, w http.ResponseWriter, e, reason string) {
	m := map[string]string{"error": e, "reason": reason}
	mustEncode(status, w, m)
}

func staticHandler(parts []string, w http.ResponseWriter, req *http.Request) {
	w.Header().Del("Content-type")
	http.StripPrefix("/_static/",
		http.FileServer(http.Dir(*staticPath))).ServeHTTP(w, req)
}

func reservedHandler(parts []string, w http.ResponseWriter, req *http.Request) {
	emitError(400,
		w, "illegal_database_name",
		"Only lowercase characters (a-z), digits (0-9), "+
			"and any of the characters _, $, (, ), +, -, and / are allowed. "+
			"Must begin with a letter.")

}

func defaultHandler(parts []string, w http.ResponseWriter, req *http.Request) {
	emitError(400,

		w, "no_handler",
		fmt.Sprintf("Can't handle %v to %v\n", req.Method, req.URL.Path))

}

func findHandler(method, path string) (routingEntry, []string) {
	for _, r := range routingTable {
		if r.Method == method {
			matches := r.Path.FindAllStringSubmatch(path, 1)
			if len(matches) > 0 {
				return r, matches[0][1:]
			}
		}
	}
	return routingEntry{"DEFAULT", nil, defaultHandler, defaultDeadline},
		[]string{}
}

func handler(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	start := time.Now()
	route, hparts := findHandler(req.Method, req.URL.Path)
	wd := time.AfterFunc(route.Deadline, func() {
		log.Printf("%v:%v is taking longer than %v",
			req.Method, req.URL.Path, route.Deadline)
	})

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-type", "application/json")
	route.Handler(hparts, w, req)

	if !wd.Stop() {
		log.Printf("%v:%v eventually finished in %v",
			req.Method, req.URL.Path, time.Since(start))
	}
}

func main() {
	halfProcs := runtime.GOMAXPROCS(0) / 2
	if halfProcs < 1 {
		halfProcs = 1
	}
	queryWorkers := flag.Int("queryWorkers", halfProcs,
		"Number of query tree walkers.")
	docWorkers := flag.Int("docWorkers", halfProcs,
		"Number of document mapreduce workers.")

	addr := flag.String("addr", ":3133", "Address to bind to")
	flag.Parse()

	// Update the query handler deadline to the query timeout
	found := false
	for i := range routingTable {
		matches := routingTable[i].Path.FindAllStringSubmatch("/x/_query", 1)
		if len(matches) > 0 {
			routingTable[i].Deadline = *queryTimeout
			found = true
			break
		}
	}
	if !found {
		log.Fatalf("Programming error:  Could not find query handler")
	}

	processorInput = make(chan processIn, *docBacklog)
	for i := 0; i < *docWorkers; i++ {
		go docProcessor(processorInput)
	}

	queryInput = make(chan *queryIn, *queryBacklog)
	for i := 0; i < *queryWorkers; i++ {
		go queryExecutor(queryInput)
	}

	s := &http.Server{
		Addr:        *addr,
		Handler:     http.HandlerFunc(handler),
		ReadTimeout: 5 * time.Second,
	}
	log.Printf("Listening to web requests on %s", *addr)
	log.Fatal(s.ListenAndServe())
}
