package main

import (
	"errors"
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
}

var unparseableTimestamp = errors.New("unparsable timestamp")

func parseTime(in string) (time.Time, error) {
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
		default:
			// second timestamps
			return time.Unix(n, 0), nil
		}
	}
	for _, f := range timeFormats {
		parsed, err := time.Parse(f, in)
		if err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, unparseableTimestamp
}
