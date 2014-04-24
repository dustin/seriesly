package main

import (
	"net/http"
	"testing"
)

func TestCanGZip(t *testing.T) {
	tests := []struct {
		headers http.Header
		can     bool
	}{
		{http.Header{}, false},
		{http.Header{"Accept-Encoding": nil}, false},
		{http.Header{"Accept-Encoding": []string{"x"}}, false},
		// Should this one be true?
		{http.Header{"Accept-Encoding": []string{"x", "gzip"}}, false},
		{http.Header{"Accept-Encoding": []string{"gzip"}}, true},
	}

	for _, test := range tests {
		req := &http.Request{Header: test.headers}
		if canGzip(req) != test.can {
			t.Errorf("Expected %v with headers %v",
				test.can, test.headers)
		}
	}
}
