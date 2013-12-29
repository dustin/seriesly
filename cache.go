package main

import (
	"hash/fnv"
	"log"
	"math"
	"strconv"
	"time"

	"github.com/dustin/gojson"
	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/client"
)

var cacheInput chan *processIn
var cacheInputSet chan *processOut

func cacheReceiveLoop(client *memcached.Client, out chan *processOut) {
	for {
		res, err := memcached.UnwrapMemcachedError(client.Receive())
		if err != nil {
			log.Printf("Error receiving from memcache: %v", err)
			return
		}

		po := processOut{cacheOpaque: res.Opaque}

		if res.Opcode == gomemcached.GET && res.Status == gomemcached.SUCCESS {
			rv := map[string][]interface{}{}
			err = json.Unmarshal(res.Body, &rv)
			if err == nil {
				po.value = rv["v"]
			} else {
				log.Printf("Decoding error:  %v\n%s", err, res.Body)
				po.err = err
			}
		} else {
			po.err = res
		}

		out <- &po
	}
}

func cacheOpener(ch chan<- *memcached.Client) {
	log.Printf("Connecting to memcached")
	client, err := memcached.Connect("tcp", *cacheAddr)
	if err != nil {
		log.Printf("Failed to connect to memcached: %v", err)
	}
	ch <- client
}

func cacheProcessor(ch <-chan *processIn, chset <-chan *processOut) {
	omap := map[uint32]*processIn{}
	opaque := uint32(100)

	out := make(chan *processOut)
	connch := make(chan *memcached.Client)

	go cacheOpener(connch)

	var client *memcached.Client

	for {
		select {
		case client = <-connch:
			if client == nil {
				time.AfterFunc(time.Second, func() {
					cacheOpener(connch)
				})
			} else {
				go cacheReceiveLoop(client, out)
			}
		case pi := <-ch:
			switch {
			case client == nil:
				// No connection, pass through
				processorInput <- pi
			case time.Now().Before(pi.before):
				newOpaque := opaque
				opaque++
				if opaque == math.MaxUint32 {
					opaque = 100
				}
				omap[newOpaque] = pi
				pi.cacheKey = cacheKey(pi)

				req := &gomemcached.MCRequest{
					Opcode: gomemcached.GET,
					Opaque: newOpaque,
					Key:    []byte(pi.cacheKey),
				}

				err := client.Transmit(req)
				if err != nil {
					log.Printf("Error transmitting!: %v", err)
					client.Close()
					client = nil
					go cacheOpener(connch)
					for _, v := range omap {
						processorInput <- v
					}
					omap = map[uint32]*processIn{}
				}
			default:
				// Too old.
				pi.out <- &processOut{"", pi.key, nil, errTimeout, 0}
			}
		case po := <-out:
			pi, ok := omap[po.cacheOpaque]
			if ok {
				delete(omap, po.cacheOpaque)
				po.key = pi.key
				if po.err == nil {
					po.cacheKey = pi.cacheKey
					pi.out <- po
				} else {
					processorInput <- pi
				}
			} else {
				if po.cacheOpaque > 10 {
					log.Printf("Unknown opaque:  %v", po.cacheOpaque)
				}
			}
		case po := <-chset:
			if client != nil {
				bytes, err := json.Marshal(po)
				if err == nil {
					req := &gomemcached.MCRequest{
						Opcode: gomemcached.SETQ,
						Key:    []byte(po.cacheKey),
						Cas:    0,
						Opaque: 1,
						Extras: []byte{0, 0, 0, 0, 0, 0, 0, 0},
						Body:   bytes}

					err = client.Transmit(req)
					if err != nil {
						log.Printf("Error transmitting!: %v", err)
						client.Close()
						client = nil
						go cacheOpener(connch)
						for _, v := range omap {
							processorInput <- v
						}
						omap = map[uint32]*processIn{}
					}
				} else {
					log.Fatalf("Error marshaling %v: %v", po, err)
				}
			} // has client, will cache
		}
	}
}

func cacheKey(p *processIn) string {
	h := fnv.New64()
	for _, pair := range p.docs {
		h.Write(pair.k)
	}
	for i := range p.ptrs {
		h.Write([]byte(p.ptrs[i]))
		h.Write([]byte(p.reds[i]))
	}
	for i := range p.filters {
		h.Write([]byte(p.filters[i]))
		h.Write([]byte(p.filtervals[i]))
	}
	return p.dbname + "#" + strconv.FormatInt(p.key, 10) +
		"#" + strconv.FormatUint(h.Sum64(), 10)
}
