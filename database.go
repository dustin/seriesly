package main

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dustin/go-couchstore"
)

func dbPath(n string) string {
	return filepath.Join(*dbRoot, n) + ".couch"
}

func dbBase(n string) string {
	left := 0
	right := len(n)
	if strings.HasPrefix(n, *dbRoot) {
		left = len(*dbRoot)
		if n[left] == '/' {
			left++
		}
	}
	if strings.HasSuffix(n, ".couch") {
		right = len(n) - 6
	}
	return n[left:right]
}

func dbopen(name string) (*couchstore.Couchstore, error) {
	db, err := couchstore.Open(dbPath(name), false)
	return db, err
}

func dbcreate(path string) error {
	db, err := couchstore.Open(path, true)
	if err != nil {
		return err
	}
	db.Close()
	return nil
}

func dbdelete(dbname string) error {
	return os.Remove(dbPath(dbname))
}

func dblist(root string) []string {
	rv := []string{}
	filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if !info.IsDir() && strings.HasSuffix(p, ".couch") {
			rv = append(rv, dbBase(p))
		}
		return err
	})
	return rv
}

func dbstore(dbname string, k string, body []byte) error {
	db, err := dbopen(dbname)
	if err != nil {
		return err
	}
	defer db.Close()
	defer db.Commit()

	return db.Set(couchstore.NewDocInfo(k, couchstore.DocIsCompressed),
		couchstore.NewDocument(k, string(body)))
}

func parseKey(s string) int64 {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return -1
	}
	return t.UnixNano()
}
