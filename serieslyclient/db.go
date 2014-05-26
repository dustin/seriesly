package serieslyclient

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/dustin/httputil"
	"github.com/dustin/seriesly/timelib"
)

// SerieslyDB provides all the DB-specific operations.
type SerieslyDB struct {
	s  *Seriesly
	db string
}

// Name returns the name of this database.
func (s *SerieslyDB) Name() string {
	return s.db
}

// URL returns a copy of the Seriesly client's URL pointed at a given DB
func (s *SerieslyDB) URL() *url.URL {
	rv := *s.s.u
	rv.Path = "/" + s.db
	return &rv
}

// Info returns the info for a database.
func (s *SerieslyDB) Info() (*DBInfo, error) {
	rv := &DBInfo{}
	res, err := s.s.client.Get(s.URL().String())
	if err != nil {
		return rv, err
	}
	if res.StatusCode != 200 {
		return rv, httputil.HTTPError(res)
	}
	defer res.Body.Close()

	err = json.NewDecoder(res.Body).Decode(rv)
	return rv, err
}

// Compact the given database.
func (s *SerieslyDB) Compact() error {
	u := s.URL().String() + "/_compact"
	req, err := http.NewRequest("POST", u, nil)
	if err != nil {
		return err
	}
	res, err := s.s.client.Do(req)
	if err != nil {
		return err
	}
	res.Body.Close()
	if res.StatusCode != 200 {
		return httputil.HTTPErrorf(res, "error compacting: %S -- %B")
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
func (s *SerieslyDB) Dump(w io.Writer, from, to string) (int64, error) {
	u := s.URL()
	u.Path += "/_dump"
	params := url.Values{}
	if err := setTimeParam(params, "from", from); err != nil {
		return 0, err
	}
	if err := setTimeParam(params, "to", to); err != nil {
		return 0, err
	}

	u.RawQuery = params.Encode()

	res, err := s.s.client.Get(u.String())
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return 0, httputil.HTTPError(res)
	}

	return io.Copy(w, res.Body)
}
