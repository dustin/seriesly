package serieslyclient

import (
	"fmt"
	"net/url"
	"strconv"
	"time"
)

// Field represents a JSON pointer field and reducer for a query.
type Field struct {
	Pointer, Reducer string
}

// Filter represents an exact-match in a query.
type Filter struct {
	Pointer, Match string
}

// Query represents a seriesly query.
type Query struct {
	From, To time.Time
	Group    time.Duration
	Fields   []Field
	Filters  []Filter
}

func (q *Query) validate() error {
	if q.Group < 1 {
		return fmt.Errorf("Grouping value must be >0, was %v", q.Group)
	}
	if len(q.Fields) == 0 {
		return fmt.Errorf("Need at least one field, have %v",
			len(q.Fields))
	}
	for _, f := range q.Fields {
		if !validReducer(f.Reducer) {
			return fmt.Errorf("Invalid reducer: %v", f.Reducer)
		}
	}
	return nil
}

func validReducer(r string) bool {
	return true
}

// Params converts this Query to query parameters.
func (q *Query) Params() url.Values {
	rv := url.Values{}
	rv.Set("group", strconv.FormatUint(uint64(q.Group/time.Millisecond), 10))
	if !q.From.IsZero() {
		rv.Set("from", q.From.Format(time.RFC3339Nano))
	}
	if !q.To.IsZero() {
		rv.Set("to", q.To.Format(time.RFC3339Nano))
	}
	for _, f := range q.Fields {
		rv["ptr"] = append(rv["ptr"], f.Pointer)
		rv["reducer"] = append(rv["reducer"], f.Reducer)
	}
	for _, f := range q.Filters {
		rv["f"] = append(rv["f"], f.Pointer)
		rv["fv"] = append(rv["fv"], f.Match)
	}
	return rv
}
