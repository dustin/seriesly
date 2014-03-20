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

func TestDBWClose(t *testing.T) {
	w := dbWriter{"test", make(chan dbqitem), make(chan bool), nil}
	err := w.Close()
	if err != nil {
		t.Errorf("First close expected success, got %v", err)
	}
	err = w.Close()
	if err != errClosed {
		t.Errorf("Expected second close to fail, got %v", err)
	}
}

func BenchmarkKeyParsing(b *testing.B) {
	input := "2012-08-26T20:46:01.911627314Z"

	for i := 0; i < b.N; i++ {
		parseKey(input)
	}
}
