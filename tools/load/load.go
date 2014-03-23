package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/dustin/seriesly/timelib"
)

var min = flag.String("min", "", "minimum timestamp (RFC3339)")

func maybeFatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func setupDb(u string) {
	req, err := http.NewRequest("PUT", u, nil)
	maybeFatal(err)
	res, err := http.DefaultClient.Do(req)
	maybeFatal(err)
	res.Body.Close()
}

func sendOne(u, k string, body []byte) {
	resp, err := http.DefaultClient.Post(u+"?ts="+k,
		"application/json", bytes.NewReader(body))
	maybeFatal(err)
	defer resp.Body.Close()
	if resp.StatusCode >= 300 || resp.StatusCode < 200 {
		log.Fatalf("HTTP Error on %v: %v", k, err)
	}
}

func parseMinTime() time.Time {
	tm, err := timelib.ParseTime(*min)
	if err != nil {
		tm = time.Time{}
	}
	return tm
}

func main() {
	flag.Parse()

	if flag.NArg() < 1 {
		log.Fatalf("Usage:  gzip -dc backup.gz | %v http://seriesly:3133/dbname",
			os.Args[0])
	}
	u := flag.Arg(0)
	setupDb(u)

	minTime := parseMinTime()

	t := time.Tick(5 * time.Second)
	i := 0

	d := json.NewDecoder(os.Stdin)
	for {
		kv := map[string]*json.RawMessage{}

		err := d.Decode(&kv)
		if err == io.EOF {
			log.Printf("Done!")
			break
		}
		maybeFatal(err)

		var latestKey string
		for k, v := range kv {
			if !minTime.IsZero() {
				thist, err := timelib.ParseTime(k)
				if err == nil && minTime.After(thist) {
					continue
				}
			}
			body := []byte(*v)
			sendOne(u, k, body)
			latestKey = k
		}

		i++
		select {
		case <-t:
			log.Printf("Processed %v items, latest was %v", i, latestKey)
		default:
		}
	}
}
