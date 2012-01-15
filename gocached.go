package main

import (
	"flag"
	"os"
	"log"
	"net"
  "runtime"
)

//global logger
var logger = log.New(os.Stdout, "gocached: ", log.Lshortfile|log.LstdFlags)


func main() {
	var storage CacheStorage
	var factory CacheStorageFactory

  runtime.GOMAXPROCS(2)
	// command line flags and parsing
	var port = flag.String("port", "11212", "memcached port")

  //	var storage_choice = flag.String("storage", "generational",
//		"storage implementation (generational, heap, map)")
//	var expiring_frequency = flag.Int64("expiring-interval", 10,
//		"expiring interval in seconds")

var partitions = flag.Int("partitions", 10, "storage partitions (0 or 1 to disable)")
	flag.Parse()

  /*
	// storage implementation selection
	switch *storage_choice {
	case "map":
		logger.Print("warning, map storage does not expire entries")
		factory = func () Storage { return newMapStorage() }
	case "generational":
		factory = func () Storage { return newGenerationalStorage() }
	case "heap":
		factory = func () Storage { return newNotifyStorage(*expiring_frequency) }
	default:
		logger.Fatalln("Invalid storage selection")
	}
*/
	// whether using partitioned or standalone storage

	if *partitions > 1 {
    logger.Printf("Building storage with partitioning support: %d slots", *partitions)
    updatesChannel := make(chan UpdateMessage, 5000)
    factory = func() CacheStorage { return newMapCacheStorage() }
    //go updateMessageLogger(updatesChannel)
    hashingStorage := newHashingStorage(uint32(*partitions), factory)
    storage = newEventNotifierStorage(hashingStorage, updatesChannel)
    newGenerationalStorage(hashingStorage, updatesChannel)
	} else {
		storage = newMapCacheStorage()//factory()
	}

	// network setup
	if addr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:"+*port); err != nil {
		logger.Fatalf("Unable to resolv local port %s\n", *port)
	} else if listener, err := net.ListenTCP("tcp", addr); err != nil {
		logger.Fatalln("Unable to listen on requested port")
	} else {
    // server loop
    logger.Printf("Starting Gocached server")
    for {
      if conn, err := listener.AcceptTCP(); err != nil {
        logger.Println("An error ocurred accepting a new connection")
      } else {
        go clientHandler(conn, storage)
      }
    }
  }
}

func clientHandler(conn *net.TCPConn, store CacheStorage) {
	defer conn.Close()
	if session, err := NewSession(conn, store); err != nil {
		logger.Println("An error ocurred creating a new session")
	} else {
    session.CommandLoop()
	}
}
