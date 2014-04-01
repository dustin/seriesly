package main

import (
	"flag"
	"log"
	"sync"
	"time"

	"github.com/dustin/seriesly/serieslyclient"
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

func vlog(s string, a ...interface{}) {
	if *verbose {
		log.Printf(s, a...)
	}
}

func compact(wg *sync.WaitGroup, s *serieslyclient.Seriesly, ch <-chan string) {
	defer wg.Done()

	for db := range ch {
		start := time.Now()
		vlog("Compacting %v", db)
		if !*noop {
			s.DB(db).Compact()
		}
		vlog("Finished compacting %v in %v", db, time.Since(start))
	}
}

func main() {
	flag.Parse()

	if flag.NArg() == 0 {
		log.Fatalf("Seriesly URL required")
	}

	s, err := serieslyclient.New(flag.Arg(0))
	maybeFatal(err, "Parsing %v: %v", flag.Arg(0), err)

	wg := &sync.WaitGroup{}
	ch := make(chan string)

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go compact(wg, s, ch)
	}

	dbs := flag.Args()[1:]
	if len(dbs) == 0 {
		dbs, err = s.List()
		maybeFatal(err, "Error listing DBs: %v", err)
	}

	for _, db := range dbs {
		ch <- db
	}
	close(ch)

	wg.Wait()
}
