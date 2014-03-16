package timelib

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
	exampleTime, err = time.Parse(time.RFC3339, exampleTimeString)
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
		tm, err := ParseTime(x.input)
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

func TestCanonicalParser(t *testing.T) {
	tests := []struct {
		input string
		exp   string
	}{
		{"2012-08-28T21:24:35.374651883Z", ""},
		{"2012-08-28T21:24:35.37465188Z", ""},
		{"2012-08-28T21:24:35.3746518Z", ""},
		{"2012-08-28T21:24:35.374651Z", ""},
		{"2012-08-28T21:24:35.37465Z", ""},
		{"2012-08-28T21:24:35.3746Z", ""},
		{"2012-08-28T21:24:35.374Z", ""},
		{"2012-08-28T21:24:35.37Z", ""},
		{"2012-08-28T21:24:35.3Z", ""},
		{"2012-08-28T21:24:35.0Z", "2012-08-28T21:24:35Z"},
		{"2012-08-28T21:24:35.Z", "2012-08-28T21:24:35Z"},
		{"2012-08-28T21:24:35Z", ""},
	}

	for _, x := range tests {
		tm, err := ParseCanonicalTime(x.input)
		if err != nil {
			t.Errorf("Error on %v - %v", x.input, err)
			t.Fail()
		}
		got := tm.UTC().Format(time.RFC3339Nano)
		exp := x.exp
		if exp == "" {
			exp = x.input
		}
		if exp != got {
			t.Errorf("Expected %v for %v, got %v", x.exp, x.input, got)
			t.Fail()
		}
	}
}

func TestCanonicalParsingErrors(t *testing.T) {
	tests := []string{
		"ZZZZZZZZZZZZZZZZZZZZ",
		"ZZZZ-ZZ-ZZTZZ:ZZ:ZZZ",
		"2014-ZZ-ZZTZZ:ZZ:ZZZ",
		"2014-03-ZZTZZ:ZZ:ZZZ",
		"2014-03-14TZZ:ZZ:ZZZ",
		"2014-03-14T15:ZZ:ZZZ",
		"2014-03-14T15:09:ZZZ",
		"2014-03-14T15:09:26.S35897Z",
	}

	for _, test := range tests {
		tm, err := ParseCanonicalTime(test)
		if err == nil {
			t.Errorf("No error on %q, got %v", test, tm)
		}
	}
}

func TestUnparseable(t *testing.T) {
	tm, err := ParseTime("an hour ago")
	if err != errUnparseableTimestamp {
		t.Fatalf("Expected unparseable, got %v/%v", tm, err)
	}
}

func benchTimeParsing(b *testing.B, input string) {
	for i := 0; i < b.N; i++ {
		_, err := ParseTime(input)
		if err != nil {
			b.Fatalf("Error on %v - %v", input, err)
		}
	}
}

func BenchmarkParseTimeCanonicalDirect(b *testing.B) {
	input := "2012-08-28T21:24:35.37465188Z"
	for i := 0; i < b.N; i++ {
		_, err := ParseCanonicalTime(input)
		if err != nil {
			b.Fatalf("Error on %v - %v", input, err)
		}
	}
}

func BenchmarkParseTimeCanonicalStdlib(b *testing.B) {
	input := "2012-08-28T21:24:35.37465188Z"
	for i := 0; i < b.N; i++ {
		_, err := time.Parse(time.RFC3339, input)
		if err != nil {
			b.Fatalf("Error on %v - %v", input, err)
		}
	}
}

func BenchmarkParseTimeCanonical(b *testing.B) {
	benchTimeParsing(b, "2012-08-28T21:24:35.37465188Z")
}

func BenchmarkParseTimeMisc(b *testing.B) {
	benchTimeParsing(b, "Tue, 28 Aug 2012 21:24:35 +0000")
}

func BenchmarkParseTimeIntNano(b *testing.B) {
	benchTimeParsing(b, "1346189075374651880")
}

func BenchmarkParseTimeIntMillis(b *testing.B) {
	benchTimeParsing(b, "1346189075374")
}

func BenchmarkParseTimeIntSecs(b *testing.B) {
	benchTimeParsing(b, "1346189075")
}
