package main

import (
	"log"
	"testing"
	"time"
)

const exampleTimeString = "2012-08-28T21:24:35.37465188Z"
const milliAccuracy = "2012-08-28T21:24:35.374Z"
const secondAccuracy = "2012-08-28T21:24:35Z"

var exampleTime time.Time

func init() {
	var err error
	exampleTime, err = time.Parse(time.RFC3339Nano, exampleTimeString)
	if err != nil {
		panic(err)
	}
	if exampleTimeString != exampleTime.UTC().Format(time.RFC3339Nano) {
		log.Panicf("Expected %v, got %v", exampleTimeString,
			exampleTime.UTC().Format(time.RFC3339Nano))
	}
}

func TestTimeParsing(t *testing.T) {
	tests := []struct {
		input string
		exp   string
	}{
		{"1346189075374651880", exampleTimeString},
		{"1346189075374", milliAccuracy},
		{"1346189075", secondAccuracy},
		{"2012-08-28T21:24:35.37465188Z", exampleTimeString},
		{secondAccuracy, secondAccuracy},
		{"Tue, 28 Aug 2012 21:24:35 +0000", secondAccuracy},
		{"Tue, 28 Aug 2012 21:24:35 UTC", secondAccuracy},
		{"Tue Aug 28 21:24:35 UTC 2012", secondAccuracy},
		{"Tue Aug 28 21:24:35 2012", secondAccuracy},
		{"Tue Aug 28 21:24:35 +0000 2012", secondAccuracy},
		{"2012-08-28T21:24", "2012-08-28T21:24:00Z"},
		{"2012-08-28T21", "2012-08-28T21:00:00Z"},
		{"2012-08-28", "2012-08-28T00:00:00Z"},
		{"2012-08", "2012-08-01T00:00:00Z"},
		{"2012", "2012-01-01T00:00:00Z"},
	}

	for _, x := range tests {
		tm, err := parseTime(x.input)
		if err != nil {
			t.Errorf("Error on %v - %v", x.input, err)
			t.Fail()
		}
		got := tm.UTC().Format(time.RFC3339Nano)
		if x.exp != got {
			t.Errorf("Expected %v for %v, got %v", x.exp, x.input, got)
			t.Fail()
		}
	}
}
