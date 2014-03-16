// Package timelib provides time parsing functions.
package timelib

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
)

var timeFormats = []string{
	time.RFC3339Nano,
	time.RFC3339,
	time.RFC1123Z,
	time.RFC1123,
	time.UnixDate,
	time.ANSIC,
	time.RubyDate,
	"2006-01-02T15:04",
	"2006-01-02T15",
	"2006-01-02",
	"2006-01",
	"2006",
}

var errUnparseableTimestamp = errors.New("unparsable timestamp")

var powTable = []int{
	10e8,
	10e7,
	10e6,
	10e5,
	10e4,
	10e3,
	10e2,
	10e1,
	10,
	1,
}

// ParseCanonicalTime parses the canonical seriesly time format.
//
// This is a hand crafted parser since it's a really common path and I
// could make it faster this way.
func ParseCanonicalTime(in string) (time.Time, error) {
	if len(in) < 20 || in[len(in)-1] != 'Z' {
		return time.Time{}, errUnparseableTimestamp
	}

	if !(in[4] == '-' && in[7] == '-' && in[10] == 'T' &&
		in[13] == ':' && in[16] == ':' && (in[19] == '.' || in[19] == 'Z')) {
		return time.Time{}, fmt.Errorf("positionally incorrect: %v", in)
	}

	// 2012-08-28T21:24:35.37465188Z
	//     4  7  10 13 16 19
	// -----------------------------
	// 0-4  5  8  11 14 17 20

	year, err := strconv.Atoi(in[0:4])
	if err != nil {
		return time.Time{}, fmt.Errorf("error parsing year: %v", err)
	}

	month, err := strconv.Atoi(in[5:7])
	if err != nil {
		return time.Time{}, fmt.Errorf("error parsing month: %v", err)
	}

	day, err := strconv.Atoi(in[8:10])
	if err != nil {
		return time.Time{}, fmt.Errorf("error parsing day: %v", err)
	}

	hour, err := strconv.Atoi(in[11:13])
	if err != nil {
		return time.Time{}, fmt.Errorf("error parsing hour: %v", err)
	}

	minute, err := strconv.Atoi(in[14:16])
	if err != nil {
		return time.Time{}, fmt.Errorf("error parsing minute: %v", err)
	}

	second, err := strconv.Atoi(in[17:19])
	if err != nil {
		return time.Time{}, fmt.Errorf("error parsing second: %v", err)
	}

	var nsecstr string
	if in[19] != 'Z' {
		nsecstr = in[20 : len(in)-1]
	}
	var nsec int

	if nsecstr != "" {
		nsec, err = strconv.Atoi(nsecstr)
		if err != nil {
			return time.Time{}, fmt.Errorf("error parsing nanoseconds: %v", err)
		}
	}

	nsec *= powTable[len(nsecstr)]

	return time.Date(year, time.Month(month), day,
		hour, minute, second, nsec, time.UTC), nil
}

// ParseTime parses any time format supported by seriesly quickly and
// easily.
//
// The following timestamps are supported:
//
//    2012-08-28T21:24:35.37465188Z - RFC3339 (this is the canonical format)
//    1346189075374651880 - nanoseconds since 1970-1-1
//    1346189075374 - milliseconds since 1970-1-1, common in java
//    1346189075 - seconds since 1970-1-1, common in unix
//    2012-08-28T21:24:35Z - RFC3339
//    Tue, 28 Aug 2012 21:24:35 +0000 - RFC1123 + numeric timezone
//    Tue, 28 Aug 2012 21:24:35 UTC RFC1123
//    Tue Aug 28 21:24:35 UTC 2012 - Unix date
//    Tue Aug 28 21:24:35 2012 - ansi C timestamp
//    Tue Aug 28 21:24:35 +0000 2012 - ruby datestamp
//    2012-08-28T21:24 - parsed as 2012-08-28T21:24:00Z
//    2012-08-28T21 - parsed as 2012-08-28T21:00:00Z
//    2012-08-28 - parsed as 2012-08-28T00:00:00Z
//    2012-08 - parsed as 2012-08-01T00:00:00Z
//    2012 - parsed as 2012-01-01T00:00:00Z
func ParseTime(in string) (time.Time, error) {
	// First, try a few numerics
	n, err := strconv.ParseInt(in, 10, 64)
	if err == nil {
		switch {
		case n > int64(math.MaxInt32)*1000:
			// nanosecond timestamps
			return time.Unix(n/1e9, n%1e9), nil
		case n > int64(math.MaxInt32):
			// millisecond timestamps
			return time.Unix(n/1000, (n%1000)*1e6), nil
		case n > 10000:
			// second timestamps
			return time.Unix(n, 0), nil
		}
	}
	rv, err := ParseCanonicalTime(in)
	if err == nil {
		return rv, nil
	}
	for _, f := range timeFormats {
		parsed, err := time.Parse(f, in)
		if err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, errUnparseableTimestamp
}
