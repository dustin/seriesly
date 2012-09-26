package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"runtime/pprof"
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
var cacheAddr = flag.String("memcache", "", "Memcached server to connect to")
var cacheBacklog = flag.Int("cacheBacklog", 1000, "Cache backlog size")
var cacheWorkers = flag.Int("cacheWorkers", 4, "Number of cache workers")

// Profiling
var pprofFile = flag.String("proFile", "", "File to write profiling info into")
var pprofStart = flag.Duration("proStart", 5*time.Second,
	"How long after startup to start profiling")
var pprofDuration = flag.Duration("proDuration", 5*time.Minute,
	"How long to run the profiler before shutting it down")

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
	routingEntry{"DELETE", regexp.MustCompile("^/(" + dbMatch + ")/_bulk$"),
		deleteBulk, *queryTimeout},
	routingEntry{"GET", regexp.MustCompile("^/(" + dbMatch + ")/_all"),
		allDocs, *queryTimeout},
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

func startProfiler() {
	time.Sleep(*pprofStart)
	log.Printf("Starting profiler")
	f, err := os.OpenFile(*pprofFile, os.O_WRONLY|os.O_CREATE, 0666)
	if err == nil {
		err = pprof.StartCPUProfile(f)
		if err != nil {
			log.Fatalf("Can't start profiler")
		}
		time.AfterFunc(*pprofDuration, func() {
			log.Printf("Shutting down profiler")
			pprof.StopCPUProfile()
			f.Close()
		})
	} else {
		log.Printf("Can't open profilefile")
	}

}

func createRootIfMissing() {
	_, err := os.Stat(*dbRoot)
	if err != nil {
		if os.IsNotExist(err) {
			var perm os.FileMode = 0777
			if err := os.Mkdir(*dbRoot, perm); err != nil {
				log.Fatal("Failed to create %s", *dbRoot)
			}
		}
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
	mcaddr := flag.String("memcbind", "", "Memcached server bind address")
	flag.Parse()
	createRootIfMissing()
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

	processorInput = make(chan *processIn, *docBacklog)
	for i := 0; i < *docWorkers; i++ {
		go docProcessor(processorInput)
	}

	if *cacheAddr == "" {
		cacheInput = processorInput
		// Note: cacheInputSet will be null here, there should be no caching
	} else {
		cacheInput = make(chan *processIn, *cacheBacklog)
		cacheInputSet = make(chan *processOut, *cacheBacklog)
		for i := 0; i < *cacheWorkers; i++ {
			go cacheProcessor(cacheInput, cacheInputSet)
		}
	}

	queryInput = make(chan *queryIn, *queryBacklog)
	for i := 0; i < *queryWorkers; i++ {
		go queryExecutor(queryInput)
	}

	if *pprofFile != "" {
		go startProfiler()
	}

	if *mcaddr != "" {
		go listenMC(*mcaddr)
	}

	s := &http.Server{
		Addr:        *addr,
		Handler:     http.HandlerFunc(handler),
		ReadTimeout: 5 * time.Second,
	}
	log.Printf("Listening to web requests on %s", *addr)
	log.Fatal(s.ListenAndServe())
}
