package main

import (
	"os"
)

type Storage interface {
  // Store this data
  Set(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (err os.Error)

  // Store this data, but only if the server *doesn't* already hold data for this key
  Add(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (err os.Error)

  // Store this data, but only if the server *does* already hold data for this key
  Replace(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (err os.Error)

  // Add this data to an existing key after existing data
  Append(key string, bytes uint32, content []byte) (err os.Error)

  // Add this data to an existing key before existing data
  Prepend(key string, bytes uint32, content []byte) (err os.Error)

  // Check and set (CAS) operation which means "store this data but
  // only if no one else has updated since I last fetched it"
  Cas(key string, flags uint32, exptime uint32, bytes uint32, cas_unique uint64, content []byte) (err os.Error)

  // Retrieve the stored data for a given key 
  Get(key string) (flags uint32, bytes uint32, cas_unique uint64, content []byte, err os.Error)

  // Delete the stored data for a given key 
  Delete(key string) (flags uint32, bytes uint32, cas_unique uint64, content []byte, err os.Error)

  // Change data for some item in-place, incrementing or decrementing it.
  // The data for the item is treated as decimal representation of a 64-bit unsigned integer.  
  // If the current data value does not conform to such a representation, returns an error.
  // Also, the item must already exist for incr/decr to work; these commands won't pretend
  // that a non-existent key exists with value 0; instead, they will fail. 
  Incr(key string, value uint64, incr bool) (result uint64, err os.Error)
}
