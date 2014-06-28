package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

var (
	pollFreq = flag.Duration("freq", 5*time.Second,
		"How often to poll URLs (0 == one-shot)")
	responseTimeout = flag.Duration("hdrtimeout", 500*time.Millisecond,
		"HTTP response header timeout")
	verbose = flag.Bool("v", false, "verbose logging")
)

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage:  %v [flags] fromurl tourl\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Flags:\n")
		flag.PrintDefaults()
	}
}

func httpCopy(dest, src string) error {
	sres, err := http.Get(src)
	if err != nil {
		return err
	}
	defer sres.Body.Close()
	if sres.StatusCode != 200 {
		return fmt.Errorf("HTTP error getting src data from %v: %v", src, sres.Status)
	}

	dres, err := http.Post(dest, sres.Header.Get("Content-Type"), sres.Body)
	if err != nil {
		return err
	}
	defer dres.Body.Close()

	if dres.StatusCode != 201 {
		errmsg, _ := ioutil.ReadAll(io.LimitReader(dres.Body, 512))
		return fmt.Errorf("HTTP Error posting result to %v: %v\n%s",
			dest, dres.StatusCode, errmsg)
	}
	return nil
}

func poll(tourl, fromurl string, t time.Time) {
	if *verbose {
		log.Printf("Copying from %v to %v", fromurl, tourl)
		defer func() { log.Printf("Finished copy in %v", time.Since(t)) }()
	}
	du := mustParseURL(tourl)
	du.RawQuery = "ts=" + strconv.FormatInt(t.UnixNano(), 10)

	if err := httpCopy(du.String(), fromurl); err != nil {
		log.Printf("Error copying data: %v", err)
	}
}

func mustParseURL(ustr string) *url.URL {
	u, e := url.Parse(ustr)
	if e != nil {
		log.Fatalf("Error parsing URL %q: %v", ustr, e)
	}
	return u
}

func initHTTP() {
	http.DefaultClient = &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives:     true,
			ResponseHeaderTimeout: *responseTimeout,
		},
	}
}

func main() {
	flag.Parse()

	if flag.NArg() < 2 {
		flag.Usage()
		os.Exit(64)
	}

	fromurl, tourl := flag.Arg(0), flag.Arg(1)

	initHTTP()

	poll(tourl, fromurl, time.Now())
	if *pollFreq > 0 {
		for t := range time.Tick(*pollFreq) {
			poll(tourl, fromurl, t)
		}
	}
}
