# A Timeseries Database

seriesly is a database for storing and querying time series data.
Unlike databases like RRDtool, it's schemaless so you can just lob
data into it and start hacking.  However, it also doesn't use a finite
amount of space.  Tradeoffs.

Detailed docs are in [the wiki][wiki].

# Quick Start

## Prereqs:

To build the software, you need the [couchstore][couchstore] library
installed and a [go][go] runtime.

## Installation

    go get github.com/dustin/seriesly

## Running

Seriesly will use as many cores as you have to offer.  Set the
`GOMAXPROCS` environment variable to the number of cores (or hardware
threads) you've got.  There's a lot of fine-grained tuning you can
employ to specify how to map the software needs to your hardware, but
the defaults should work quite well.

Then just start blasting data into it.  See the [protocol docs][wiki]
for details on this.

[wiki]: //github.com/dustin/seriesly/wiki
[couchstore]: /couchbase/couchstore
[go]: http://golang.org/
