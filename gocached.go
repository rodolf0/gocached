package main

import (
	"flag"
	"net"
	"strconv"
	"os"
	"log"
)

var service_port = flag.Int("port", 11212, "memcached port")
/*var max_memory = flag.Int("maxmem", 1024, "max MB to cache")*/

var logger = log.New(os.Stdout, "", 0)

////////////////////////////////////////////////////////////////

func main() {
	
  logger.Printf("Starting Gocached server")

  addr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:"+strconv.Itoa(*service_port))
  assertNoError(err)
  listener, err := net.ListenTCP("tcp", addr)
  assertNoError(err)

  logger.Printf("Listening on %s:%d", "0.0.0.0", *service_port)

  for {
    var tcp_conn, err = listener.AcceptTCP()
      if err != nil {
        continue
      } else {
        go connectionHandler(tcp_conn)
      }
  }
}

func connectionHandler(tcp_conn *net.TCPConn) {

  defer tcp_conn.Close()

  commandReader, _ := NewCommandReader(tcp_conn)

  for {
    cmd, err := commandReader.Read()
    if err != nil {
      logger.Print("Connection closed by remote client")
      return
    }
    if cmd != nil {
      cmd.Exec()
    }
  }
}

func assertNoError(err os.Error) {
  if err != nil {
    panic(err)
  }
}

