package main

import (
	"testing"
	"time"
)

var testTime = time.Date(2014, 3, 16, 23, 9, 11, 82859, time.UTC)

func TestFormats(t *testing.T) {
	tests := map[string]string{
		"":                          "",
		"plain":                     "plain",
		"per%%cent":                 "per%cent",
		"%n-thing":                  "dbname-thing",
		"dump/%y/%m/%d-%H-%M-%S-%n": "dump/2014/3/16-23-09-11-dbname",
	}

	for in, exp := range tests {
		got := format(in, "dbname", testTime)
		if got != exp {
			t.Errorf("Failed on %q, got %q, expected %q", in, got, exp)
		}
	}
}
