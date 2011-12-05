package main

import (
  "flag"
  "net"
  "fmt"
)

var port = flag.String("port", "11211", "memcached port")


func main() {
  if addr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:" + *port); err != nil {
    panic("Unable to resolv local port: " + *port)
  } else if listener, err := net.ListenTCP("tcp", addr); err != nil {
    panic("Unable to listen on requested port")
  } else {
    for {
      if conn, err := listener.AcceptTCP(); err != nil {
        fmt.Println("An error ocurred accepting a connection")
      } else {
        go clientHandler(conn)
      }
    }
  }
}


func clientHandler(conn *net.TCPConn) {
  defer conn.Close()

  if session, err := NewSession(conn); err != nil {
    fmt.Println("An error ocurred creating a session")
  } else {
    for {
      session.NextCommand()
    }
  }
}
