package main

import (
  "flag"
  "os"
	"log"
  "net"
)

var logger = log.New(os.Stdout, "gocached: ", log.Lshortfile | log.LstdFlags)
var port = flag.String("port", "11212", "memcached port")


func main() {
  newGenerationalStorage()
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
        go func(connection *net.TCPConn) {
          defer connection.Close()
          if session, err := NewSession(connection); err != nil {
            logger.Println("An error ocurred creating a new session")
          } else {
            session.CommandLoop()
          }
        }(conn)
      }
    }
  }
}
