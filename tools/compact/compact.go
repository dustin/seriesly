package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

var verbose = flag.Bool("v", false, "verbosity")
var noop = flag.Bool("n", false, "don't actually compact")
var concurrency = flag.Int("j", 2,
	"number of concurrent compactions")

func init() {
	log.SetFlags(log.Lmicroseconds)
}

func maybeFatal(err error, fmt string, args ...interface{}) {
	if err != nil {
		log.Fatalf(fmt, args...)
	}
}

func listDatabases(u url.URL) []string {
	u.Path = "/_all_dbs"
	res, err := http.Get(u.String())
	maybeFatal(err, "Error listing databases: %v", err)
	defer res.Body.Close()

	rv := []string{}
	d := json.NewDecoder(res.Body)
	err = d.Decode(&rv)
	maybeFatal(err, "Error decoding database list: %v", err)
	return rv
}

func vlog(s string, a ...interface{}) {
	if *verbose {
		log.Printf(s, a...)
	}
}

func compact(wg *sync.WaitGroup, u url.URL, ch <-chan string) {
	defer wg.Done()

	for db := range ch {
		start := time.Now()
		vlog("Compacting %v", db)
		if !*noop {
			u.Path = fmt.Sprintf("/%v/_compact", db)
			req, err := http.NewRequest("POST", u.String(), nil)
			maybeFatal(err, "Error creating request: %v", err)
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				vlog("Error compacting %v: %v", db, err)
			}
			res.Body.Close()
			if res.StatusCode != 200 {
				vlog("Error compacting %v: %v", db,
					res.Status)
			}
		}
		vlog("Finished compacting %v in %v", db, time.Since(start))
	}
}

func main() {
	flag.Parse()

	if flag.NArg() == 0 {
		log.Fatalf("Seriesly URL required")
	}

	u, err := url.Parse(flag.Arg(0))
	maybeFatal(err, "Parsing %v: %v", flag.Arg(0), err)

	wg := &sync.WaitGroup{}
	ch := make(chan string)

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go compact(wg, *u, ch)
	}

	for _, db := range listDatabases(*u) {
		ch <- db
	}
	close(ch)

	wg.Wait()
}
