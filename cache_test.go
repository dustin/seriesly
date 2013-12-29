package main

import (
	"testing"
	"time"
)

func benchCacheSize(b *testing.B, num int) {
	p := processIn{
		dbname: "mydatabase",
		key:    817492945,
		ptrs:   []string{"/some/pointer", "/other/pointer"},
		reds:   []string{"min", "max"},
	}

	startTime := time.Now()
	for i := 0; i < num; i++ {
		k := startTime.Format(time.RFC3339Nano)
		p.docs = append(p.docs, kvpair{[]byte(k), nil})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cacheKey(&p)
	}
}

func BenchmarkCacheKeying1440(b *testing.B) {
	benchCacheSize(b, 1440)
}

func BenchmarkCacheKeying10(b *testing.B) {
	benchCacheSize(b, 10)
}
