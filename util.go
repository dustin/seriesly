package main

import (
	"reflect"
)

// Thanks to remy_o in #go-nuts for this.
func closeAll(chans interface{}) {
	v := reflect.ValueOf(chans)
	for i, imax := 0, v.Len(); i < imax; i++ {
		v.Index(i).Close()
	}
}
