package main

import (
	"compress/gzip"
	"flag"
	"io"
	"log"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/dustin/httputil"
	"github.com/dustin/seriesly/serieslyclient"
)

var (
	verbose     = flag.Bool("v", false, "verbosity")
	concurrency = flag.Int("j", 2, "number of concurrent dumps")
	dbName      = flag.String("db", "", "which db to dump (default: all)")
	noop        = flag.Bool("n", false, "if true, don't actually write dumps")
	from        = flag.String("from", "", "oldest key to dump")
	to          = flag.String("to", "", "newest key to dump")
	formatStr   = flag.String("format", "%n.json.gz", "dump name format")
)

const sigInfo = syscall.Signal(29)

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

func compress(w io.Writer) io.WriteCloser {
	z, err := gzip.NewWriterLevel(w, gzip.BestCompression)
	maybeFatal(err, "NewWriterLevel: %v", err)
	return z
}

func dumpOne(s *serieslyclient.SerieslyDB, t time.Time) (int64, error) {
	fn := format(*formatStr, s.Name(), t)
	outf, err := os.Create(fn)
	if err != nil {
		return 0, err
	}
	defer outf.Close()

	z := compress(outf)
	defer z.Close()

	return s.Dump(z, *from, *to)
}

func dump(wg *sync.WaitGroup, s *serieslyclient.Seriesly, ch <-chan string) {
	defer wg.Done()

	t := time.Now()
	for db := range ch {
		start := time.Now()
		vlog("Dumping %v", db)
		n, err := dumpOne(s.DB(db), t)
		maybeFatal(err, "Error dumping %v: %v", db, err)

		if !*noop {
			vlog("Dumped %v of %v in %v",
				humanize.Bytes(uint64(n)), db, time.Since(start))
		}
	}
}

func main() {
	flag.Parse()

	httputil.InitHTTPTracker(false)

	if flag.NArg() == 0 {
		log.Fatalf("Seriesly URL required")
	}

	s, err := serieslyclient.New(flag.Arg(0))
	maybeFatal(err, "Parsing %v: %v", flag.Arg(0), err)

	wg := &sync.WaitGroup{}
	ch := make(chan string)

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go dump(wg, s, ch)
	}

	if *dbName == "" {
		dbs, err := s.List()
		maybeFatal(err, "Error listing: %v", err)
		for _, db := range dbs {
			ch <- db
		}
	} else {
		ch <- *dbName
	}
	close(ch)

	wg.Wait()
}
