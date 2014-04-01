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
	u      *url.URL
	client *http.Client
}

// New creates a new Seriesly instance for the given URL.
func New(u string) (*Seriesly, error) {
	return NewWithClient(u, http.DefaultClient)
}

// NewWithClient creates a new Seriesly instance with the specified
// HTTP client.
func NewWithClient(u string, client *http.Client) (*Seriesly, error) {
	rvu, err := url.Parse(u)
	if err != nil {
		return nil, err
	}
	return &Seriesly{rvu, client}, nil
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
	res, err := s.client.Get(u.String())
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

// DB returns a SerieslyDB instance for the given DB.
func (s *Seriesly) DB(db string) *SerieslyDB {
	return &SerieslyDB{s, db}
}
