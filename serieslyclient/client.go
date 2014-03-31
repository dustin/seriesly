// Package serieslyclient provides access to a seriesly instance.
package serieslyclient

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/dustin/seriesly/timelib"
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

// Info returns the info for a database.
func (s *Seriesly) Info(dbname string) (*DBInfo, error) {
	u := *s.u
	u.Path = "/" + dbname
	rv := &DBInfo{}
	res, err := s.client.Get(u.String())
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
	res, err := s.client.Do(req)
	if err != nil {
		return err
	}
	res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("HTTP error compacting: %v", res.Status)
	}
	return nil
}

func setTimeParam(uv url.Values, name, val string) error {
	if val == "" {
		return nil
	}
	t, err := timelib.ParseTime(val)
	if err != nil {
		return err
	}
	uv.Set(name, strconv.FormatInt(t.UnixNano(), 10))
	return nil
}

// Dump a database or range of a database to a Writer.
//
// db is the name of the db to dump
// from and to are both optional and will be parsed as a seriesly
// timestamp.
func (s *Seriesly) Dump(w io.Writer, db, from, to string) (int64, error) {
	u := *s.u
	u.Path = "/" + db + "/_dump"
	params := url.Values{}
	if err := setTimeParam(params, "from", from); err != nil {
		return 0, err
	}
	if err := setTimeParam(params, "to", to); err != nil {
		return 0, err
	}

	u.RawQuery = params.Encode()

	res, err := s.client.Get(u.String())
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return 0, fmt.Errorf("HTTP Error: %v", res.Status)
	}

	return io.Copy(w, res.Body)
}
