package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"regexp"
)

var dbRoot = flag.String("root", "db", "Root directory for database files.")

type routeHandler func(parts []string, w http.ResponseWriter, req *http.Request)

type routingEntry struct {
	Method  string
	Path    *regexp.Regexp
	Handler routeHandler
}

const dbMatch = "[-%+()$_a-z0-9]+"

var routingTable []routingEntry = []routingEntry{
	routingEntry{"GET", regexp.MustCompile("^/$"), serverInfo},
	// Database stuff
	routingEntry{"GET", regexp.MustCompile("^/_all_dbs$"), listDatabases},
	routingEntry{"GET", regexp.MustCompile("^/_(.*)"), reservedHandler},
	routingEntry{"GET", regexp.MustCompile("^/(" + dbMatch + ")/?$"), dbInfo},
	routingEntry{"HEAD", regexp.MustCompile("^/(" + dbMatch + ")/?$"), checkDB},
	routingEntry{"GET", regexp.MustCompile("^/(" + dbMatch + ")/_changes$"), dbChanges},
	routingEntry{"GET", regexp.MustCompile("^/(" + dbMatch + ")/_query$"), query},
	routingEntry{"PUT", regexp.MustCompile("^/(" + dbMatch + ")/?$"), createDB},
	routingEntry{"DELETE", regexp.MustCompile("^/(" + dbMatch + ")/?$"), deleteDB},
	routingEntry{"POST", regexp.MustCompile("^/(" + dbMatch + ")/?$"), newDocument},
	// Document stuff
	routingEntry{"PUT", regexp.MustCompile("^/(" + dbMatch + ")/([^/]+)$"), putDocument},
	routingEntry{"GET", regexp.MustCompile("^/(" + dbMatch + ")/([^/]+)$"), getDocument},
	routingEntry{"DELETE", regexp.MustCompile("^/(" + dbMatch + ")/([^/]+)$"), rmDocument},
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
	return routingEntry{"DEFAULT", nil, defaultHandler}, []string{}
}

func handler(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	route, hparts := findHandler(req.Method, req.URL.Path)
	log.Printf("Handling %v:%v", req.Method, req.URL.Path)
	route.Handler(hparts, w, req)
}

func main() {
	addr := flag.String("addr", ":3133", "Address to bind to")
	flag.Parse()

	s := &http.Server{
		Addr:    *addr,
		Handler: http.HandlerFunc(handler),
	}
	log.Printf("Listening to web requests on %s", *addr)
	log.Fatal(s.ListenAndServe())
}
