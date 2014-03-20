package main

import (
	"testing"
)

func TestKeyParsing(t *testing.T) {
	tests := map[string]int64{
		"2012-08-26T20:46:01.911627314Z": 1346013961911627314,
		"82488858158":                    -1,
	}

	for input, exp := range tests {
		got := parseKey(input)
		if got != exp {
			t.Errorf("Expected %v, got %v", exp, got)
		}
	}
}

func BenchmarkKeyParsing(b *testing.B) {
	input := "2012-08-26T20:46:01.911627314Z"

	for i := 0; i < b.N; i++ {
		parseKey(input)
	}
}
