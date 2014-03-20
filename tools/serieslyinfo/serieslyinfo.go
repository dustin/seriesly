package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"net/url"
	"os"

	"github.com/dustin/go-humanize"
)

var verbose = flag.Bool("v", false, "verbosity")
var noop = flag.Bool("n", false, "don't actually compact")

const defaultTemplate = `{{.dbname}}:
  Space Used:       {{.info.SpaceUsed|bytes}}
  Last Sequence:    {{.info.SpaceUsed|comma}}
  Header Position:  {{.info.HeaderPos|comma}}
  Document Count:   {{.info.DocCount|comma}}
  Deleted Count:    {{.info.DeletedCount|comma}}

`

type dbinfo struct {
	SpaceUsed    json.Number `json:"space_used"`
	LastSeq      json.Number `json:"last_seq"`
	HeaderPos    json.Number `json:"header_pos"`
	DocCount     json.Number `json:"doc_count"`
	DeletedCount json.Number `json:"deleted_count"`
	Error        string
}

var tmpl = template.Must(template.New("").Funcs(template.FuncMap{
	"comma": func(n json.Number) string {
		nint, err := n.Int64()
		maybeFatal(err, "Invalid int64: %v: %v", n, err)
		return humanize.Comma(nint)
	},
	"bytes": func(n json.Number) string {
		nint, err := n.Int64()
		maybeFatal(err, "Invalid int64: %v: %v", n, err)
		return humanize.Bytes(uint64(nint))
	},
}).Parse(defaultTemplate))

func init() {
	log.SetFlags(log.Lmicroseconds)
}

func maybeFatal(err error, fmt string, args ...interface{}) {
	if err != nil {
		log.Fatalf(fmt, args...)
	}
}

func listDatabases(u url.URL) []string {
	u.Path = "/_all_dbs"
	res, err := http.Get(u.String())
	maybeFatal(err, "Error listing databases: %v", err)
	defer res.Body.Close()

	rv := []string{}
	d := json.NewDecoder(res.Body)
	err = d.Decode(&rv)
	maybeFatal(err, "Error decoding database list: %v", err)
	return rv
}

func fetchDBInfo(u url.URL, which string) (dbinfo, error) {
	u.Path = "/" + which
	rv := dbinfo{}
	res, err := http.Get(u.String())
	if err != nil {
		return rv, err
	}
	if res.StatusCode != 200 {
		return rv, fmt.Errorf("HTTP error:  %v", res.Status)
	}
	defer res.Body.Close()

	err = json.NewDecoder(res.Body).Decode(&rv)
	return rv, err
}

func vlog(s string, a ...interface{}) {
	if *verbose {
		log.Printf(s, a...)
	}
}

func describe(base url.URL, db string) {
	di, err := fetchDBInfo(base, db)
	maybeFatal(err, "Couldn't fetch info for %v: %v", db, err)
	tmpl.Execute(os.Stdout, map[string]interface{}{
		"dbname": db,
		"info":   di,
	})
}

func main() {
	flag.Parse()

	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(64)
	}

	base, err := url.Parse(flag.Arg(0))
	maybeFatal(err, "Couldn't parse URL: %v", err)

	dbs := flag.Args()[1:]
	if len(dbs) == 0 {
		dbs = listDatabases(*base)
	}

	for _, d := range dbs {
		describe(*base, d)
	}
}
