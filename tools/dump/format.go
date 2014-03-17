package main

import (
	"bytes"
	"strconv"
	"strings"
	"text/scanner"
	"time"
)

func twodig(s string) string {
	if len(s) == 1 {
		return "0" + s
	}
	return s
}

func format(str, dbname string, tm time.Time) string {
	buf := bytes.Buffer{}

	var s scanner.Scanner
	s.Init(strings.NewReader(str))
	s.Mode = 0
	s.Whitespace = 0
	for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
		if tok != '%' {
			buf.WriteRune(tok)
			continue
		}

		switch s := s.Scan(); s {
		case '%':
			buf.WriteRune('%')
		case 'n':
			buf.WriteString(dbname)
		case 'Y', 'y':
			buf.WriteString(strconv.Itoa(tm.Year()))
		case 'm':
			buf.WriteString(strconv.Itoa(int(tm.Month())))
		case 'd':
			buf.WriteString(strconv.Itoa(tm.Day()))
		case 'H':
			buf.WriteString(twodig(strconv.Itoa(tm.Hour())))
		case 'M':
			buf.WriteString(twodig(strconv.Itoa(tm.Minute())))
		case 'S':
			buf.WriteString(twodig(strconv.Itoa(tm.Second())))
		}
	}

	return buf.String()
}
