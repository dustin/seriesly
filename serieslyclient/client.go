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

// URL returns a copy of the Seriesly client's URL.
func (s *Seriesly) URL() *url.URL {
	rv := *s.u
	return &rv
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

// Compact the given database.
func (s *Seriesly) Compact(db string) error {
	u := *s.u
	u.Path = fmt.Sprintf("/%v/_compact", db)
	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("HTTP error compacting: %v", res.Status)
	}
	return nil
}
