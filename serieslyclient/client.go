// Package serieslyclient provides access to a seriesly instance.
package serieslyclient

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

// A Seriesly DB.
type Seriesly struct {
	u *url.URL
}

// New creates a new Seriesly instance for the given URL.
func New(u string) (*Seriesly, error) {
	rvu, err := url.Parse(u)
	if err != nil {
		return nil, err
	}
	return &Seriesly{rvu}, nil
}

// DBInfo represents database info.
type DBInfo struct {
	DBName       string
	SpaceUsed    json.Number `json:"space_used"`
	LastSeq      json.Number `json:"last_seq"`
	HeaderPos    json.Number `json:"header_pos"`
	DocCount     json.Number `json:"doc_count"`
	DeletedCount json.Number `json:"deleted_count"`
	Error        string
}

// List all databases.
func (s *Seriesly) List() ([]string, error) {
	u := *s.u
	u.Path = "/_all_dbs"
	res, err := http.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP error: %v", err)
	}

	rv := []string{}
	err = json.NewDecoder(res.Body).Decode(&rv)
	return rv, err
}

// Info returns the info for a database.
func (s *Seriesly) Info(dbname string) (*DBInfo, error) {
	u := *s.u
	u.Path = "/" + dbname
	rv := &DBInfo{}
	res, err := http.Get(u.String())
	if err != nil {
		return rv, err
	}
	if res.StatusCode != 200 {
		return rv, fmt.Errorf("HTTP error:  %v", res.Status)
	}
	defer res.Body.Close()

	err = json.NewDecoder(res.Body).Decode(rv)
	return rv, err
}
