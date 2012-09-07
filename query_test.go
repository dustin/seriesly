package main

import (
	"encoding/json"
	"io/ioutil"
	"reflect"
	"testing"
)

var testInput = []*string{}

var bigInput []byte

func init() {
	testInput = append(testInput, nil)
	s := []string{"31", "63", "foo"}
	for i := range s {
		testInput = append(testInput, &s[i])
	}

	var err error
	bigInput, err = ioutil.ReadFile("sample.json")
	if err != nil {
		panic("Couldn't read sample.json")
	}
}

func streamCollection(s []*string) chan *string {
	ch := make(chan *string)
	go func() {
		defer close(ch)
		for _, r := range s {
			ch <- r
		}
	}()
	return ch
}

func TestReducers(t *testing.T) {

	tests := []struct {
		reducer string
		exp     interface{}
	}{
		{"any", "31"},
		{"count", 3},
		{"sum", float64(94)},
		{"sumsq", float64(4930)},
		{"max", float64(63)},
		{"min", float64(31)},
		{"avg", 94.0 / 2.0},
		{"identity", testInput},
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

func BenchmarkJSONParser(b *testing.B) {
	b.SetBytes(int64(len(bigInput)))
	for i := 0; i < b.N; i++ {
		m := map[string]interface{}{}
		err := json.Unmarshal(bigInput, &m)
		if err != nil {
			b.Fatalf("Error unmarshaling json: %v", err)
		}
	}
}
