package main

import (
	"testing"
)

func TestKeyParsing(t *testing.T) {
	input := "2012-08-26T20:46:01.911627314Z"
	exp := int64(1346013961911627314)

	got := parseKey(input)
	if got != exp {
		t.Fatalf("Expected %v, got %v", exp, got)
	}
}
