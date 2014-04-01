package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"os"
	"text/tabwriter"

	"github.com/dustin/go-humanize"
	"github.com/dustin/seriesly/serieslyclient"
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

func vlog(s string, a ...interface{}) {
	if *verbose {
		log.Printf(s, a...)
	}
}

func describe(s *serieslyclient.Seriesly, dbs ...string) <-chan *serieslyclient.DBInfo {
	rv := make(chan *serieslyclient.DBInfo)
	go func() {
		defer close(rv)
		for _, db := range dbs {
			di, err := s.DB(db).Info()
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

	s, err := serieslyclient.New(flag.Arg(0))
	maybeFatal(err, "Couldn't set up client: %v", err)

	dbs := flag.Args()[1:]
	if len(dbs) == 0 {
		dbs, err = s.List()
		maybeFatal(err, "Error listing DBs: %v", err)
	}

	tsrc := defaultTemplate
	if *short {
		tsrc = shortTemplate
	}

	tmpl := getTemplate(tsrc)

	tw := tabwriter.NewWriter(os.Stdout, 8, 8, 2, ' ', 0)
	tmpl.Execute(tw, describe(s, dbs...))
	defer tw.Flush()
}
