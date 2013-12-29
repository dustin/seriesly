package main

import (
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
)

var verbose = flag.Bool("v", false, "verbosity")
var concurrency = flag.Int("j", 2,
	"number of concurrent dumps")

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

func compress(w io.Writer) io.WriteCloser {
	z, err := gzip.NewWriterLevel(w, gzip.BestCompression)
	maybeFatal(err, "NewWriterLevel: %v", err)
	return z
}

func dumpOne(dbname, u string) (int64, error) {
	res, err := http.Get(u)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return 0, fmt.Errorf("HTTP Error: %v", res.Status)
	}

	outf, err := os.Create(dbname + ".json.gz")
	if err != nil {
		return 0, err
	}
	defer outf.Close()

	z := compress(outf)
	defer z.Close()
	return io.Copy(z, res.Body)
}

func dump(wg *sync.WaitGroup, u url.URL, ch <-chan string) {
	defer wg.Done()

	for db := range ch {
		start := time.Now()
		vlog("Dumping %v", db)
		u.Path = "/" + db + "/_all"
		n, err := dumpOne(db, u.String())
		maybeFatal(err, "Error dumping %v: %v", u.String(), err)

		vlog("Dumped %v of %v in %v",
			humanize.Bytes(uint64(n)), db, time.Since(start))
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
		go dump(wg, *u, ch)
	}

	for _, db := range listDatabases(*u) {
		ch <- db
	}
	close(ch)

	wg.Wait()
}
