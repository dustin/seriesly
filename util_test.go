package main

import (
	"testing"
)

func TestCloseAllEmpty(t *testing.T) {
	closeAll([]chan int{})
}

func TestCloseAllTwo(t *testing.T) {
	ichan := make(chan int)
	i2chan := make(chan int)
	closeAll([]chan int{ichan, i2chan})
	_, ok := <-ichan
	if ok {
		t.Errorf("ichan wasn't closed")
	}
	_, ok = <-i2chan
	if ok {
		t.Errorf("bchan wasn't closed")
	}
}

func buildTestChans(n int) []chan int {
	s := make([]chan int, 0, n)
	for i := 0; i < n; i++ {
		s = append(s, make(chan int))
	}
	return s
}

func runBenchmarkCloseRange(b *testing.B, n int) {
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		s := buildTestChans(n)

		b.StartTimer()
		for i := range s {
			close(s[i])
		}
		b.StopTimer()
	}
}

func runBenchmarkCloseAll(b *testing.B, n int) {
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		s := buildTestChans(n)

		b.StartTimer()
		closeAll(s)
		b.StopTimer()
	}
}

func BenchmarkCloseRange1(b *testing.B) {
	runBenchmarkCloseRange(b, 1)
}

func BenchmarkCloseRange3(b *testing.B) {
	runBenchmarkCloseRange(b, 3)
}

func BenchmarkCloseRange1000(b *testing.B) {
	runBenchmarkCloseRange(b, 1000)
}

func BenchmarkCloseAll1(b *testing.B) {
	runBenchmarkCloseAll(b, 1)
}

func BenchmarkCloseAll3(b *testing.B) {
	runBenchmarkCloseAll(b, 3)
}

func BenchmarkCloseAll1000(b *testing.B) {
	runBenchmarkCloseAll(b, 1000)
}
