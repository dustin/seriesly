package main

import (
	"reflect"
	"testing"
)

var testInput = []*string{}

func init() {
	testInput = append(testInput, nil)
	s := []string{"31", "63", "foo"}
	for i := range s {
		testInput = append(testInput, &s[i])
	}
}

func TestReducers(t *testing.T) {

	tests := []struct {
		reducer string
		exp     interface{}
	}{
		{"any", "31"},
		{"count", 3},
		{"sum", int64(94)},
		{"max", int64(63)},
		{"min", int64(31)},
		{"avg", 94.0 / 2.0},
		{"identity", testInput},
	}

	for _, test := range tests {
		got := reducers[test.reducer](testInput)
		if !reflect.DeepEqual(got, test.exp) {
			t.Errorf("Expected %v for %v, got %v",
				test.exp, test.reducer, got)
			t.Fail()
		}
	}
}
