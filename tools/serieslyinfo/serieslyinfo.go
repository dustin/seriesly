package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"text/tabwriter"

	"github.com/dustin/go-humanize"
)

var (
	verbose      = flag.Bool("v", false, "verbose")
	short        = flag.Bool("short", false, "short form")
	templateSrc  = flag.String("t", "", "Display template")
	templateFile = flag.String("T", "", "Display template filename")
)

const defaultTemplate = `{{range .}}
{{.DBName}}:
  Space Used:       {{.SpaceUsed|bytes}}
  Last Sequence:    {{.SpaceUsed|comma}}
  Header Position:  {{.HeaderPos|comma}}
  Document Count:   {{.DocCount|comma}}
  Deleted Count:    {{.DeletedCount|comma}}
{{end}}
`

const shortTemplate = `dbname	docs	space used
------	----	----------
{{range .}}{{.DBName}}	{{.DocCount|comma}}	{{.SpaceUsed|bytes}}
{{end}}`

type dbinfo struct {
	DBName       string
	SpaceUsed    json.Number `json:"space_used"`
	LastSeq      json.Number `json:"last_seq"`
	HeaderPos    json.Number `json:"header_pos"`
	DocCount     json.Number `json:"doc_count"`
	DeletedCount json.Number `json:"deleted_count"`
	Error        string
}

var funcMap = template.FuncMap{
	"comma": func(n json.Number) string {
		nint, err := n.Int64()
		maybeFatal(err, "Invalid int64: %v: %v", n, err)
		return humanize.Comma(nint)
	},
	"bytes": func(n json.Number) string {
		nint, err := n.Int64()
		maybeFatal(err, "Invalid int64: %v: %v", n, err)
		return humanize.Bytes(uint64(nint))
	}}

func init() {
	log.SetFlags(log.Lmicroseconds)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %v [-v] http://serieslyhost:3133/ [dbnames...]\n",
			os.Args[0])
		flag.PrintDefaults()
	}
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

func describe(base url.URL, dbs ...string) <-chan dbinfo {
	rv := make(chan dbinfo)
	go func() {
		defer close(rv)
		for _, db := range dbs {
			di, err := fetchDBInfo(base, db)
			maybeFatal(err, "Couldn't fetch info for %v: %v", db, err)
			di.DBName = db
			rv <- di
		}
	}()
	return rv
}

func getTemplate(tdefault string) *template.Template {
	tmplstr := *templateSrc
	if tmplstr == "" {
		switch *templateFile {
		case "":
			tmplstr = tdefault
		case "-":
			td, err := ioutil.ReadAll(os.Stdin)
			maybeFatal(err, "Error reading template from stdin: %v", err)
			tmplstr = string(td)
		default:
			td, err := ioutil.ReadFile(*templateFile)
			maybeFatal(err, "Error reading template file: %v", err)
			tmplstr = string(td)
		}
	}

	tmpl, err := template.New("").Funcs(funcMap).Parse(tmplstr)
	maybeFatal(err, "Error parsing template: %v", err)
	return tmpl
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

	tsrc := defaultTemplate
	if *short {
		tsrc = shortTemplate
	}

	tmpl := getTemplate(tsrc)

	tw := tabwriter.NewWriter(os.Stdout, 8, 8, 2, ' ', 0)
	tmpl.Execute(tw, describe(*base, dbs...))
	defer tw.Flush()
}
