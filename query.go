package main

import (
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/dustin/go-jsonpointer"
	"github.com/dustin/gojson"
)

var errTimeout = errors.New("query timed out")

type ptrval struct {
	key      []byte
	val      interface{}
	included bool
}

type reducer func(input chan ptrval) interface{}

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

type kvpair struct {
	k, v []byte
}

type processIn struct {
	cacheKey   string
	dbname     string
	key        int64
	docs       []kvpair
	nextKey    []byte
	ptrs       []string
	reds       []string
	before     time.Time
	filters    []string
	filtervals []string
	out        chan<- *processOut
}

type queryIn struct {
	dbname     string
	from       string
	to         string
	group      int
	ptrs       []string
	reds       []string
	start      time.Time
	before     time.Time
	filters    []string
	filtervals []string
	started    int32
	totalKeys  int32
	out        chan *processOut
	cherr      chan error
}

func resolveFetch(j []byte, keys []string) map[string]interface{} {
	rv := map[string]interface{}{}
	found, err := jsonpointer.FindMany(j, keys)
	if err != nil {
		return rv
	}
	for k, v := range found {
		var val interface{}
		err = json.Unmarshal(v, &val)
		if err == nil {
			rv[k] = val
		}
	}
	return rv
}

func processDoc(key []byte, chs []chan ptrval, doc []byte, ptrs []string,
	filters []string, filtervals []string, included bool) {

	pv := ptrval{key, nil, included}

	// Find all keys for filters and comparisons so we can do a
	// single pass through the document.
	keys := make([]string, 0, len(filters)+len(ptrs))
	seen := map[string]bool{}
	for _, f := range filters {
		if !seen[f] {
			keys = append(keys, f)
			seen[f] = true
		}
	}
	for _, f := range ptrs {
		if !seen[f] {
			keys = append(keys, f)
			seen[f] = true
		}
	}

	fetched := resolveFetch(doc, keys)

	for i, p := range filters {
		val := fetched[p]
		checkVal := filtervals[i]
		switch val.(type) {
		case string:
			if val != checkVal {
				return
			}
		case int, uint, int64, float64, uint64, bool:
			v := fmt.Sprintf("%v", val)
			if v != checkVal {
				return
			}
		default:
			return
		}
	}

	for i, p := range ptrs {
		val := fetched[p]
		if p == "_id" {
			val = key
		}
		switch x := val.(type) {
		case int, uint, int64, float64, uint64, bool:
			v := fmt.Sprintf("%v", val)
			pv.val = v
			chs[i] <- pv
		default:
			pv.val = x
			chs[i] <- pv
		}
	}
}

func processDocs(pi *processIn) {

	result := processOut{pi.cacheKey, pi.key, nil, nil, 0}

	if len(pi.ptrs) == 0 {
		log.Panicf("No pointers specified in query: %#v", *pi)
	}

	db, err := dbopen(pi.dbname)
	if err != nil {
		result.err = err
		pi.out <- &result
		return
	}
	defer closeDBConn(db)

	chans := make([]chan ptrval, 0, len(pi.ptrs))
	resultchs := make([]chan interface{}, 0, len(pi.ptrs))
	for i, r := range pi.reds {
		chans = append(chans, make(chan ptrval))
		resultchs = append(resultchs, make(chan interface{}))

		go func(fi int, fr string) {
			resultchs[fi] <- reducers[fr](chans[fi])
		}(i, r)
	}

	go func() {
		defer closeAll(chans)

		for _, pair := range pi.docs {
			processDoc(pair.k, chans, pair.v, pi.ptrs,
				pi.filters, pi.filtervals, true)
		}
		if pi.nextKey != nil {
			processDoc(nil, chans, nil, pi.ptrs,
				pi.filters, pi.filtervals, false)
		}
	}()

	results := make([]interface{}, len(pi.ptrs))
	for i := 0; i < len(pi.ptrs); i++ {
		results[i] = <-resultchs[i]
		if f, fok := results[i].(float64); fok &&
			(math.IsNaN(f) || math.IsInf(f, 0)) {
			results[i] = nil
		}
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
			processDocs(pi)
		} else {
			pi.out <- &processOut{"", pi.key, nil, errTimeout, 0}
		}
	}
}

func fetchDocs(dbname string, key int64, docs []kvpair,
	nextKey []byte, ptrs []string, reds []string,
	filters []string, filtervals []string,
	before time.Time, out chan<- *processOut) {

	i := processIn{"", dbname, key, docs, nextKey,
		ptrs, reds, before, filters, filtervals, out}

	cacheInput <- &i
}

func runQuery(q *queryIn) {
	if len(q.ptrs) == 0 {
		q.cherr <- fmt.Errorf("at least one pointer is required")
		return
	}
	if q.group == 0 {
		q.cherr <- fmt.Errorf("group level cannot be zero")
		return
	}

	chunk := int64(time.Duration(q.group) * time.Millisecond)

	docs := []kvpair{}
	g := int64(0)
	nextg := ""

	err := dbwalk(q.dbname, q.from, q.to, func(k, v []byte) error {
		atomic.AddInt32(&q.totalKeys, 1)
		kstr := string(k)
		if kstr >= nextg {
			if len(docs) > 0 {
				atomic.AddInt32(&q.started, 1)
				fetchDocs(q.dbname, g, docs, k,
					q.ptrs, q.reds, q.filters, q.filtervals,
					q.before, q.out)

				docs = make([]kvpair, 0, len(docs))
			}

			k := parseKey(kstr)
			g = (k / chunk) * chunk
			nextgi := g + chunk
			nextgt := time.Unix(nextgi/1e9, nextgi%1e9).UTC()
			nextg = nextgt.Format(time.RFC3339Nano)
		}
		docs = append(docs, kvpair{k, v})
		return nil
	})

	if err == nil && len(docs) > 0 {
		atomic.AddInt32(&q.started, 1)
		fetchDocs(q.dbname, g, docs, nil,
			q.ptrs, q.reds, q.filters, q.filtervals,
			q.before, q.out)
	}

	q.cherr <- err
}

func queryExecutor() {
	for q := range queryInput {
		if time.Now().Before(q.before) {
			runQuery(q)
		} else {
			log.Printf("Timed out query that's %v late",
				time.Since(q.before))
			q.cherr <- errTimeout
		}
	}
}

func executeQuery(dbname, from, to string, group int,
	ptrs, reds, filters, filtervals []string) *queryIn {
	now := time.Now()

	rv := &queryIn{
		dbname:     dbname,
		from:       from,
		to:         to,
		group:      group,
		ptrs:       ptrs,
		reds:       reds,
		start:      now,
		before:     now.Add(*queryTimeout),
		filters:    filters,
		filtervals: filtervals,
		out:        make(chan *processOut),
		cherr:      make(chan error),
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
				switch value := v.val.(type) {
				case string:
					x, err := strconv.ParseFloat(value, 64)
					if err == nil {
						ch <- x
					}
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
	FIND_USABLE:
		for v := range in {
			if v.key != nil && v.val != nil {
				switch value := v.val.(type) {
				case string:
					x, err := strconv.ParseFloat(value, 64)
					if err == nil {
						prevts = parseKey(string(v.key))
						preval = x
						break FIND_USABLE
					}
				}
			}
		}
		// Then emit floats based on deltas from previous values.
		for v := range in {
			if v.key != nil && v.val != nil {
				switch value := v.val.(type) {
				case string:
					x, err := strconv.ParseFloat(value, 64)
					if err == nil {
						thists := parseKey(string(v.key))

						val := ((x - preval) /
							(float64(thists-prevts) / 1e9))

						if !(math.IsNaN(val) || math.IsInf(val, 0)) {
							ch <- val
						}

						prevts = thists
						preval = x
					}
				}
			}
		}
	}()

	return ch
}

var reducers = map[string]reducer{
	"identity": func(input chan ptrval) interface{} {
		rv := []interface{}{}
		for s := range input {
			if s.included {
				rv = append(rv, s.val)
			}
		}
		return rv
	},
	"any": func(input chan ptrval) interface{} {
		var rv interface{}
		for v := range input {
			if rv == nil && v.included && v.val != nil {
				rv = v.val
			}
		}
		return rv
	},
	"distinct": func(input chan ptrval) interface{} {
		uvm := map[interface{}]bool{}
		for v := range input {
			if v.included {

				switch value := v.val.(type) {
				case map[string]interface{}:
				case []interface{}:
					//unhashable
					continue
				default:
					uvm[value] = true
				}

			}
		}
		rv := make([]interface{}, 0, len(uvm))
		for k := range uvm {
			rv = append(rv, k)
		}
		return rv
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
		rv := math.NaN()
		for v := range convertTofloat64(input) {
			if v > rv || math.IsNaN(rv) || math.IsInf(rv, 0) {
				rv = v
			}
		}
		return rv
	},
	"min": func(input chan ptrval) interface{} {
		rv := math.NaN()
		for v := range convertTofloat64(input) {
			if v < rv || math.IsNaN(rv) || math.IsInf(rv, 0) {
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
		return math.NaN()
	},
	"c": func(input chan ptrval) interface{} {
		sum := float64(0)
		for v := range convertTofloat64Rate(input) {
			sum += v
		}
		return sum
	},
	"c_min": func(input chan ptrval) interface{} {
		rv := math.NaN()
		for v := range convertTofloat64Rate(input) {
			if v < rv || math.IsNaN(rv) || math.IsInf(rv, 0) {
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
		if nums > 0 {
			return sum / nums
		}
		return math.NaN()
	},
	"c_max": func(input chan ptrval) interface{} {
		rv := math.NaN()
		for v := range convertTofloat64Rate(input) {
			if v > rv || math.IsNaN(rv) || math.IsInf(rv, 0) {
				rv = v
			}
		}
		return rv
	},
	"obj_keys": func(input chan ptrval) interface{} {
		rv := []string{}
		for v := range input {
			if v.included {
				switch value := v.val.(type) {
				case map[string]interface{}:
					for mapk := range value {
						rv = append(rv, mapk)
					}
				}
			}
		}
		return rv
	},
	"obj_distinct_keys": func(input chan ptrval) interface{} {
		ukm := map[string]bool{}
		for v := range input {
			if v.included {
				switch value := v.val.(type) {
				case map[string]interface{}:
					for mapk := range value {
						ukm[mapk] = true
					}
				}
			}
		}
		rv := make([]string, 0, len(ukm))
		for k := range ukm {
			rv = append(rv, k)
		}
		return rv
	},
}
