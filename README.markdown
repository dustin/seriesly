# A Timeseries Database

seriesly is a database for storing and querying time series data.
Unlike databases like RRDtool, it's schemaless so you can just lob
data into it and start hacking.  However, it also doesn't use a finite
amount of space.  Tradeoffs.

Detailed docs are in [the wiki][wiki].

# Quick Start

## Prereqs:

To build the software, you need the latest version of [go][go].

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

# More Info

My [blog post][blog] provides an overview of the why and a little bit
of the what.  There's also a [youtube video][youtube] of me demoing
the software a few days after the initial version hit github.  It's
matured a lot since then, but the core concepts haven't changed.

[blog]: http://dustin.github.com/2012/09/09/seriesly.html
[youtube]: http://youtu.be/8b-8NTCyFQQ
[wiki]: https://github.com/dustin/seriesly/wiki
[go]: http://golang.org/
