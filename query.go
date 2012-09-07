package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/dustin/go-couchstore"
	"github.com/dustin/go-jsonpointer"
)

var timeoutError = errors.New("query timed out")

type Reducer func(input chan *string) interface{}

type processOut struct {
	cacheKey    string
	key         int64
	value       interface{}
	err         error
	cacheOpaque uint32
}

func (p processOut) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{"v": p.value})
}

type processIn struct {
	cacheKey string
	dbname   string
	key      int64
	infos    []*couchstore.DocInfo
	ptrs     []string
	reds     []string
	before   time.Time
	out      chan<- *processOut
}

type queryIn struct {
	dbname    string
	from      string
	to        string
	group     int
	ptrs      []string
	reds      []string
	start     time.Time
	before    time.Time
	started   int32
	totalKeys int32
	out       chan *processOut
	cherr     chan error
}

func processDoc(chs []chan *string, doc string, ptrs []string) {

	j := map[string]interface{}{}
	err := json.Unmarshal([]byte(doc), &j)
	if err != nil {
		for i := range ptrs {
			chs[i] <- nil
		}
		return
	}
	for i, p := range ptrs {
		val := jsonpointer.Get(j, p)
		switch x := val.(type) {
		case string:
			chs[i] <- &x
		case int, uint, int64, float64, uint64, bool:
			v := fmt.Sprintf("%v", val)
			chs[i] <- &v
		default:
			log.Printf("Ignoring %T", val)
			chs[i] <- nil
		}
	}
}

func process_docs(pi *processIn) {

	result := processOut{pi.cacheKey, pi.key, nil, nil, 0}

	db, err := dbopen(pi.dbname)
	if err != nil {
		result.err = err
		pi.out <- &result
		return
	}
	defer db.Close()

	chans := make([]chan *string, 0, len(pi.ptrs))
	resultchs := make([]chan interface{}, 0, len(pi.ptrs))
	for i, r := range pi.reds {
		chans = append(chans, make(chan *string))
		resultchs = append(resultchs, make(chan interface{}))

		go func() {
			resultchs[i] <- reducers[r](chans[i])
		}()
	}

	go func() {
		defer func() {
			for i := range chans {
				close(chans[i])
			}
		}()

		for _, di := range pi.infos {
			doc, err := db.GetFromDocInfo(di)
			if err == nil {
				processDoc(chans, doc.Value(), pi.ptrs)
			} else {
				for i := range pi.ptrs {
					chans[i] <- nil
				}
			}
		}
	}()

	results := make([]interface{}, len(pi.ptrs))
	for i := 0; i < len(pi.ptrs); i++ {
		results[i] = <-resultchs[i]
	}
	result.value = results

	if result.cacheOpaque == 0 && result.cacheKey != "" {
		// It's OK if we can't store our newly pulled item in
		// the cache, but it's most definitely not OK to stop
		// here because of this.
		select {
		case cacheInputSet <- &result:
		default:
		}
	}
	pi.out <- &result
}

func docProcessor(ch <-chan *processIn) {
	for pi := range ch {
		if time.Now().Before(pi.before) {
			process_docs(pi)
		} else {
			pi.out <- &processOut{"", pi.key, nil, timeoutError, 0}
		}
	}
}

func fetchDocs(dbname string, key int64, infos []*couchstore.DocInfo,
	ptrs []string, reds []string, before time.Time,
	out chan<- *processOut) {

	i := processIn{"", dbname, key, infos,
		ptrs, reds, before, out}

	cacheInput <- &i
}

func runQuery(q *queryIn) {
	db, err := dbopen(q.dbname)
	if err != nil {
		log.Printf("Error opening db: %v - %v", q.dbname, err)
		q.cherr <- err
		return
	}
	defer db.Close()

	chunk := int64(time.Duration(q.group) * time.Millisecond)

	infos := []*couchstore.DocInfo{}
	prevg := int64(0)

	err = db.Walk(q.from, func(d *couchstore.Couchstore,
		di *couchstore.DocInfo) error {
		if q.to != "" && di.ID() >= q.to {
			return couchstore.StopIteration
		}

		k := parseKey(di.ID())
		g := (k / chunk) * chunk
		atomic.AddInt32(&q.totalKeys, 1)

		if g != prevg && len(infos) > 0 {
			atomic.AddInt32(&q.started, 1)
			fetchDocs(q.dbname, prevg, infos,
				q.ptrs, q.reds, q.before, q.out)

			infos = make([]*couchstore.DocInfo, 0, len(infos))
		}
		infos = append(infos, di)
		prevg = g

		return nil
	})

	if err == nil && len(infos) > 0 {
		atomic.AddInt32(&q.started, 1)
		fetchDocs(q.dbname, prevg, infos,
			q.ptrs, q.reds, q.before, q.out)
	}

	q.cherr <- err
}

func queryExecutor(ch <-chan *queryIn) {
	for q := range ch {
		if time.Now().Before(q.before) {
			runQuery(q)
		} else {
			log.Printf("Timed out query that's %v late",
				time.Since(q.before))
			q.cherr <- timeoutError
		}
	}
}

func executeQuery(dbname, from, to string, group int,
	ptrs []string, reds []string) *queryIn {

	now := time.Now()

	rv := &queryIn{
		dbname: dbname,
		from:   from,
		to:     to,
		group:  group,
		ptrs:   ptrs,
		reds:   reds,
		start:  now,
		before: now.Add(*queryTimeout),
		out:    make(chan *processOut),
		cherr:  make(chan error),
	}
	queryInput <- rv
	return rv
}

var processorInput chan *processIn
var queryInput chan *queryIn

func convertTofloat64(in chan *string) chan float64 {
	ch := make(chan float64)
	go func() {
		defer close(ch)
		for v := range in {
			if v != nil {
				x, err := strconv.ParseFloat(*v, 64)
				if err == nil {
					ch <- x
				}
			}
		}
	}()

	return ch
}

var reducers = map[string]Reducer{
	"identity": func(input chan *string) interface{} {
		rv := []*string{}
		for s := range input {
			rv = append(rv, s)
		}
		return rv
	},
	"any": func(input chan *string) interface{} {
		for v := range input {
			if v != nil {
				return *v
			}
		}
		return nil
	},
	"count": func(input chan *string) interface{} {
		rv := 0
		for v := range input {
			if v != nil {
				rv++
			}
		}
		return rv
	},
	"sum": func(input chan *string) interface{} {
		rv := float64(0)
		for v := range convertTofloat64(input) {
			rv += v
		}
		return rv
	},
	"sumsq": func(input chan *string) interface{} {
		rv := float64(0)
		for v := range convertTofloat64(input) {
			rv += (v * v)
		}
		return rv
	},
	"max": func(input chan *string) interface{} {
		rv := float64(math.MinInt64)
		for v := range convertTofloat64(input) {
			if v > rv {
				rv = v
			}
		}
		return rv
	},
	"min": func(input chan *string) interface{} {
		rv := float64(math.MaxInt64)
		for v := range convertTofloat64(input) {
			if v < rv {
				rv = v
			}
		}
		return rv
	},
	"avg": func(input chan *string) interface{} {
		nums := float64(0)
		sum := float64(0)
		for v := range convertTofloat64(input) {
			nums++
			sum += v
		}
		return sum / nums
	},
}
