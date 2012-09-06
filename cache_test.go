package main

import (
	"testing"
	"time"

	"github.com/dustin/go-couchstore"
)

func BenchmarkCacheKeying(b *testing.B) {

	p := processIn{
		dbname: "mydatabase",
		key:    817492945,
		ptrs:   []string{"/some/pointer", "/other/pointer"},
		reds:   []string{"min", "max"},
	}

	startTime := time.Now()
	for i := 0; i < 1440; i++ {
		di := couchstore.NewDocInfo(startTime.Format(time.RFC3339Nano),
			0)
		p.infos = append(p.infos, di)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cacheKey(&p)
	}
}
