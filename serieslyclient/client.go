// Package serieslyclient provides access to a seriesly instance.
package serieslyclient

import (
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/dustin/httputil"
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
		return nil, httputil.HTTPError(res)
	}

	rv := []string{}
	err = json.NewDecoder(res.Body).Decode(&rv)
	return rv, err
}

// DB returns a SerieslyDB instance for the given DB.
func (s *Seriesly) DB(db string) *SerieslyDB {
	return &SerieslyDB{s, db}
}

// Create creates a new database.
func (s *Seriesly) Create(db string) error {
	u := *s.u
	u.Path = "/" + db
	req, err := http.NewRequest("PUT", u.String(), nil)
	if err != nil {
		return err
	}
	res, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 201 {
		return httputil.HTTPErrorf(res, "Error creating DB: %S -- %B")
	}
	return nil
}

// Delete destroys a database.
func (s *Seriesly) Delete(db string) error {
	u := *s.u
	u.Path = "/" + db
	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}
	res, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return httputil.HTTPErrorf(res, "Error deleting DB: %S -- %B")
	}
	return nil
}
