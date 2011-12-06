package main

import (
  "flag"
  "os"
	"log"
  "net"
)

var logger = log.New(os.Stdout, "gocached: ", log.Lshortfile | log.LstdFlags)
var port = flag.String("port", "11211", "memcached port")

func main() {
  if addr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:" + *port); err != nil {
    logger.Fatalf("Unable to resolv local port %s\n", *port)
  } else if listener, err := net.ListenTCP("tcp", addr); err != nil {
    logger.Fatalln("Unable to listen on requested port")
  } else {
    logger.Printf("Starting Gocached server")
    for {
      if conn, err := listener.AcceptTCP(); err != nil {
        logger.Println("An error ocurred accepting a new connection")
      } else {
        go clientHandler(conn)
      }
    }
  }
}


func clientHandler(conn *net.TCPConn) {
  defer conn.Close()
  if session, err := NewSession(conn); err != nil {
    logger.Println("An error ocurred creating a new session")
  } else {
    for {
      session.NextCommand().Exec(session)
    }
  }
}
