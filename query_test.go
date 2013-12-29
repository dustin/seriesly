package main

import (
	"fmt"
	"io/ioutil"
	"math"
	"reflect"
	"testing"
	"time"
)

var testInput = []interface{}{}
var nextValue = "29"

var bigInput []byte

func init() {
	s := []interface{}{"31", "63", "foo", "17",
		"foo", "foo", "foo", "foo",
		map[string]interface{}{"key": "value1"},
		map[string]interface{}{"key": "value2"},
		map[string]interface{}{"key": "value3"}}
	for i := range s {
		testInput = append(testInput, s[i])
	}

	var err error
	bigInput, err = ioutil.ReadFile("sample.json")
	if err != nil {
		panic("Couldn't read sample.json")
	}
}

func streamCollection(s []interface{}) chan ptrval {
	ch := make(chan ptrval)
	go func() {
		defer close(ch)
		t := time.Unix(1347255646, 418514126).UTC()
		for _, r := range s {
			t = t.Add(time.Second)
			ts := t.Format(time.RFC3339Nano)
			ch <- ptrval{[]byte(ts), r, true}
		}
		t = t.Add(time.Second)
		ts := t.Format(time.RFC3339Nano)
		ch <- ptrval{[]byte(ts), nextValue, false}
	}()
	return ch
}

func TestEmptyRateConversion(t *testing.T) {
	ch := make(chan ptrval)
	rch := convertTofloat64Rate(ch)
	close(ch)
	val, got := <-rch
	if got {
		t.Fatalf("Expected empty channel, got %v", val)
	}
}

func TestSingleRateConversion(t *testing.T) {
	ch := make(chan ptrval, 1)
	rch := convertTofloat64Rate(ch)
	ch <- ptrval{nil, &nextValue, true}
	close(ch)
	val, got := <-rch
	if got {
		t.Fatalf("Expected empty channel, got %v", val)
	}
}

func TestPairRateConversion(t *testing.T) {
	ch := make(chan ptrval, 2)
	rch := convertTofloat64Rate(ch)

	tm := time.Now().UTC()
	val1 := "20"
	ch <- ptrval{[]byte(tm.Format(time.RFC3339Nano)), val1, true}

	tm = tm.Add(5 * time.Second)
	val2 := "25"
	ch <- ptrval{[]byte(tm.Format(time.RFC3339Nano)), val2, false}

	close(ch)
	exp := 1.0
	val, got := <-rch
	if !got {
		t.Fatalf("Expected value, got empty channel")
	}
	if val != exp {
		t.Fatalf("Expected %v, got %v", exp, val)
	}
}

func TestReducers(t *testing.T) {
	tests := []struct {
		reducer string
		exp     interface{}
	}{
		{"any", "31"},
		{"count", 11},
		{"sum", float64(111)},
		{"sumsq", float64(5219)},
		{"max", float64(63)},
		{"min", float64(17)},
		{"avg", float64(37)},
		{"c", float64(10.5)},
		{"c_min", float64(-23)},
		{"c_avg", float64(3.5)},
		{"c_max", float64(32)},
		{"identity", testInput},
		{"obj_keys", []string{"key", "key", "key"}},
		{"obj_distinct_keys", []string{"key"}},
		// distinct reducer tested separately
	}

	for _, test := range tests {
		got := reducers[test.reducer](streamCollection(testInput))
		if !reflect.DeepEqual(got, test.exp) {
			t.Errorf("Expected %v for %v, got %v",
				test.exp, test.reducer, got)
			t.Fail()
		}
	}
}

// the order of items returned by distinct is not gauaranteed
// which makes using TestReducers problematic as is
// also numeric values become strings do to some interal behavior
// this could change over time, for now its primarily intended
// to work with string values
func TestDistinctReducer(t *testing.T) {
	test := struct {
		reducer string
		exp     []interface{}
	}{"distinct", []interface{}{"foo", "17", "31", "63"}}

	got := reducers[test.reducer](streamCollection(testInput)).([]interface{})
	if len(got) != len(test.exp) {
		t.Errorf("Expected length of result to be %v for %v, got %v",
			len(test.exp), test.reducer, len(got))
	}
OUTER:
	for _, expval := range test.exp {
		for _, gotval := range got {
			if reflect.DeepEqual(expval, gotval) {
				continue OUTER
			}
		}
		t.Errorf("Expected result to contain %v for %v, got %v",
			expval, test.reducer, got)
	}
}

func TestEmptyReducers(t *testing.T) {
	emptyInput := []interface{}{}
	tests := []struct {
		reducer string
		exp     interface{}
	}{
		{"any", nil},
		{"count", 0},
		{"sum", 0.0},
		{"sumsq", 0.0},
		{"max", math.NaN()},
		{"min", math.NaN()},
		{"avg", math.NaN()},
		{"c", 0.0},
		{"c_min", math.NaN()},
		{"c_avg", math.NaN()},
		{"c_max", math.NaN()},
		{"identity", emptyInput},
		{"obj_keys", []string{}},
		{"obj_distinct_keys", []string{}},
		{"distinct", emptyInput},
	}

	eq := func(a, b interface{}) bool {
		if !reflect.DeepEqual(a, b) {
			af, aok := a.(float64)
			bf, bok := b.(float64)
			return aok && bok && (math.IsNaN(af) == math.IsNaN(bf))
		}
		return true
	}

	for _, test := range tests {
		got := reducers[test.reducer](streamCollection(emptyInput))
		if !eq(got, test.exp) {
			t.Errorf("Expected %v for %v, got %v",
				test.exp, test.reducer, got)
			t.Fail()
		}
	}
}

func TestNilReducers(t *testing.T) {
	emptyInput := []interface{}{nil}
	tests := []struct {
		reducer string
		exp     interface{}
	}{
		{"any", nil},
		{"count", 0},
		{"sum", 0.0},
		{"sumsq", 0.0},
		{"max", math.NaN()},
		{"min", math.NaN()},
		{"avg", math.NaN()},
		{"c", 0.0},
		{"c_min", math.NaN()},
		{"c_avg", math.NaN()},
		{"c_max", math.NaN()},
		{"identity", emptyInput},
		{"obj_keys", []string{}},
		{"obj_distinct_keys", []string{}},
		{"distinct", emptyInput},
	}

	eq := func(a, b interface{}) bool {
		if !reflect.DeepEqual(a, b) {
			af, aok := a.(float64)
			bf, bok := b.(float64)
			return aok && bok && (math.IsNaN(af) == math.IsNaN(bf))
		}
		return true
	}

	for _, test := range tests {
		got := reducers[test.reducer](streamCollection(emptyInput))
		if !eq(got, test.exp) {
			t.Errorf("Expected %v for %v, got %v",
				test.exp, test.reducer, got)
			t.Fail()
		}
	}
}

func TestPointers(t *testing.T) {
	docID := "2013-02-22T16:29:19.750264Z"
	tests := []struct {
		pointer string
		exp     string
	}{
		{"/kind", "Listing"},
		{"_id", docID},
	}

	for _, test := range tests {
		chans := make([]chan ptrval, 0, 1)
		chans = append(chans, make(chan ptrval))
		go processDoc([]byte(docID), chans, bigInput, []string{test.pointer}, []string{}, []string{}, true)
		got := <-chans[0]
		if test.exp != fmt.Sprintf("%s", got.val) {
			t.Errorf("Expected %q for %v, got %q",
				test.exp, test.pointer, got.val)
		}
	}

}
