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

type ptrval struct {
	di       *couchstore.DocInfo
	val      *string
	included bool
}

type Reducer func(input chan ptrval) interface{}

type processOut struct {
	cacheKey    string
	key         int64
	value       []interface{}
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
	nextInfo *couchstore.DocInfo
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

func processDoc(di *couchstore.DocInfo, chs []chan ptrval,
	doc []byte, ptrs []string, included bool) {

	pv := ptrval{di, nil, included}

	j := map[string]interface{}{}
	err := json.Unmarshal(doc, &j)
	if err != nil {
		for i := range ptrs {
			chs[i] <- pv
		}
		return
	}
	for i, p := range ptrs {
		val := jsonpointer.Get(j, p)
		switch x := val.(type) {
		case string:
			pv.val = &x
			chs[i] <- pv
		case int, uint, int64, float64, uint64, bool:
			v := fmt.Sprintf("%v", val)
			pv.val = &v
			chs[i] <- pv
		default:
			log.Printf("Ignoring %T", val)
			chs[i] <- pv
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

	chans := make([]chan ptrval, 0, len(pi.ptrs))
	resultchs := make([]chan interface{}, 0, len(pi.ptrs))
	for i, r := range pi.reds {
		chans = append(chans, make(chan ptrval))
		resultchs = append(resultchs, make(chan interface{}))

		go func() {
			resultchs[i] <- reducers[r](chans[i])
		}()
	}

	go func() {
		defer closeAll(chans)

		dodoc := func(di *couchstore.DocInfo, included bool) {
			doc, err := db.GetFromDocInfo(di)
			if err == nil {
				processDoc(di, chans, doc.Value(), pi.ptrs, included)
			} else {
				for i := range pi.ptrs {
					chans[i] <- ptrval{di, nil, included}
				}
			}
		}

		for _, di := range pi.infos {
			dodoc(di, true)
		}
		if pi.nextInfo != nil {
			dodoc(pi.nextInfo, false)
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
	nextInfo *couchstore.DocInfo, ptrs []string, reds []string,
	before time.Time, out chan<- *processOut) {

	i := processIn{"", dbname, key, infos, nextInfo,
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
	g := int64(0)
	nextg := ""

	err = db.Walk(q.from, func(d *couchstore.Couchstore,
		di *couchstore.DocInfo) error {
		kstr := di.ID()
		var err error
		if q.to != "" && kstr >= q.to {
			err = couchstore.StopIteration
		}

		atomic.AddInt32(&q.totalKeys, 1)

		if kstr >= nextg {
			if len(infos) > 0 {
				atomic.AddInt32(&q.started, 1)
				fetchDocs(q.dbname, g, infos, di,
					q.ptrs, q.reds, q.before, q.out)

				infos = make([]*couchstore.DocInfo, 0, len(infos))
			}

			k := parseKey(kstr)
			g = (k / chunk) * chunk
			nextgi := g + chunk
			nextgt := time.Unix(nextgi/1e9, nextgi%1e9).UTC()
			nextg = nextgt.Format(time.RFC3339Nano)
		}
		infos = append(infos, di)

		return err
	})

	if err == nil && len(infos) > 0 {
		atomic.AddInt32(&q.started, 1)
		fetchDocs(q.dbname, g, infos, nil,
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

func convertTofloat64(in chan ptrval) chan float64 {
	ch := make(chan float64)
	go func() {
		defer close(ch)
		for v := range in {
			if v.included && v.val != nil {
				x, err := strconv.ParseFloat(*v.val, 64)
				if err == nil {
					ch <- x
				}
			}
		}
	}()

	return ch
}

func convertTofloat64Rate(in chan ptrval) chan float64 {
	ch := make(chan float64)
	go func() {
		defer close(ch)
		var prevts int64
		var preval float64

		// First, find a part of the stream that has usable data.
		for v := range in {
			if v.di != nil && v.val != nil {
				x, err := strconv.ParseFloat(*v.val, 64)
				if err == nil {
					prevts = parseKey(v.di.ID())
					preval = x
					break
				}
			}
		}
		// Then emit floats based on deltas from previous values.
		for v := range in {
			if v.di != nil && v.val != nil {
				x, err := strconv.ParseFloat(*v.val, 64)
				if err == nil {
					thists := parseKey(v.di.ID())

					val := ((x - preval) /
						(float64(thists-prevts) / 1e9))

					if !math.IsNaN(val) {
						ch <- val
					}

					prevts = thists
					preval = x
				}
			}
		}
	}()

	return ch
}

var reducers = map[string]Reducer{
	"identity": func(input chan ptrval) interface{} {
		rv := []*string{}
		for s := range input {
			if s.included {
				rv = append(rv, s.val)
			}
		}
		return rv
	},
	"any": func(input chan ptrval) interface{} {
		for v := range input {
			if v.included && v.val != nil {
				return *v.val
			}
		}
		return nil
	},
	"count": func(input chan ptrval) interface{} {
		rv := 0
		for v := range input {
			if v.included && v.val != nil {
				rv++
			}
		}
		return rv
	},
	"sum": func(input chan ptrval) interface{} {
		rv := float64(0)
		for v := range convertTofloat64(input) {
			rv += v
		}
		return rv
	},
	"sumsq": func(input chan ptrval) interface{} {
		rv := float64(0)
		for v := range convertTofloat64(input) {
			rv += (v * v)
		}
		return rv
	},
	"max": func(input chan ptrval) interface{} {
		rv := float64(math.MinInt64)
		for v := range convertTofloat64(input) {
			if v > rv {
				rv = v
			}
		}
		return rv
	},
	"min": func(input chan ptrval) interface{} {
		rv := float64(math.MaxInt64)
		for v := range convertTofloat64(input) {
			if v < rv {
				rv = v
			}
		}
		return rv
	},
	"avg": func(input chan ptrval) interface{} {
		nums := float64(0)
		sum := float64(0)
		for v := range convertTofloat64(input) {
			nums++
			sum += v
		}
		if nums > 0 {
			return sum / nums
		}
		return nil
	},
	"c_min": func(input chan ptrval) interface{} {
		rv := float64(math.MaxInt64)
		for v := range convertTofloat64Rate(input) {
			if v < rv {
				rv = v
			}
		}
		return rv
	},
	"c_avg": func(input chan ptrval) interface{} {
		nums := float64(0)
		sum := float64(0)
		for v := range convertTofloat64Rate(input) {
			nums++
			sum += v
		}
		return sum / nums
	},
	"c_max": func(input chan ptrval) interface{} {
		rv := float64(math.MinInt64)
		for v := range convertTofloat64Rate(input) {
			if v > rv {
				rv = v
			}
		}
		return rv
	},
}
