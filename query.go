package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strconv"

	"github.com/dustin/go-couchstore"
	"github.com/dustin/go-jsonpointer"
)

type processOut struct {
	key   int64
	value interface{}
	err   error
}

type processIn struct {
	dbname string
	key    int64
	infos  []*couchstore.DocInfo
	ptrs   []string
	reds   []Reducer
	out    chan<- processOut
}

func processDoc(collection [][]*string, doc string, ptrs []string) {

	j := map[string]interface{}{}
	err := json.Unmarshal([]byte(doc), &j)
	if err != nil {
		for i := range ptrs {
			collection[i] = append(collection[i], nil)
		}
		return
	}
	for i, p := range ptrs {
		val := jsonpointer.Get(j, p)
		switch x := val.(type) {
		case string:
			collection[i] = append(collection[i], &x)
		case int, uint, int64, float64, uint64, bool:
			v := fmt.Sprintf("%v", val)
			collection[i] = append(collection[i], &v)
		default:
			log.Printf("Ignoring %T", val)
			collection[i] = append(collection[i], nil)
		}
	}
}

func process_docs(dbname string, key int64, infos []*couchstore.DocInfo,
	ptrs []string, reds []Reducer, ch chan<- processOut) {

	log.Printf("Processing %v keys", len(infos))

	result := processOut{key, nil, nil}

	db, err := dbopen(dbname)
	if err != nil {
		result.err = err
		ch <- result
		return
	}
	defer db.Close()

	collection := make([][]*string, len(ptrs))

	for _, di := range infos {
		doc, err := db.GetFromDocInfo(di)
		if err == nil {
			processDoc(collection, doc.Value(), ptrs)
		} else {
			for i := range collection {
				collection[i] = append(collection[i], nil)
			}
		}
	}

	result.value = reduce(collection, reds)
	ch <- result
}

func docProcessor(ch <-chan processIn) {
	for pi := range ch {
		process_docs(pi.dbname, pi.key, pi.infos, pi.ptrs,
			pi.reds, pi.out)
	}
}

var processorInput chan processIn

type Reducer func(input []*string) interface{}

func reduce(collection [][]*string, reducers []Reducer) []interface{} {
	rv := make([]interface{}, len(collection))
	for i, a := range collection {
		rv[i] = reducers[i](a)
	}
	return rv
}

func convertTofloat64(in []*string) []float64 {
	rv := make([]float64, 0, len(in))
	for _, v := range in {
		if v != nil {
			x, err := strconv.ParseFloat(*v, 64)
			if err == nil {
				rv = append(rv, x)
			}
		}
	}
	return rv
}

var reducers = map[string]Reducer{
	"identity": func(input []*string) interface{} {
		return input
	},
	"any": func(input []*string) interface{} {
		for _, v := range input {
			if v != nil {
				return *v
			}
		}
		return nil
	},
	"count": func(input []*string) interface{} {
		rv := 0
		for _, v := range input {
			if v != nil {
				rv++
			}
		}
		return rv
	},
	"sum": func(input []*string) interface{} {
		rv := float64(0)
		for _, v := range convertTofloat64(input) {
			rv += v
		}
		return rv
	},
	"sumsq": func(input []*string) interface{} {
		rv := float64(0)
		for _, v := range convertTofloat64(input) {
			rv += (v * v)
		}
		return rv
	},
	"max": func(input []*string) interface{} {
		rv := float64(math.MinInt64)
		for _, v := range convertTofloat64(input) {
			if v > rv {
				rv = v
			}
		}
		return rv
	},
	"min": func(input []*string) interface{} {
		rv := float64(math.MaxInt64)
		for _, v := range convertTofloat64(input) {
			if v < rv {
				rv = v
			}
		}
		return rv
	},
	"avg": func(input []*string) interface{} {
		nums := convertTofloat64(input)
		sum := float64(0)
		for _, v := range nums {
			sum += v
		}
		return float64(sum) / float64(len(nums))
	},
}
