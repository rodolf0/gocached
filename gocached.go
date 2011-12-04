package main

import (
  "fmt"
  "flag"
  "net"
  "strconv"
  "bufio"
)

var service_port = flag.Int("port", 11211, "memcached port")
/*var max_memory = flag.Int("maxmem", 1024, "max MB to cache")*/

////////////////////////////////////////////////////////////////

func main() {

  var addr, _ = net.ResolveTCPAddr("tcp", "0.0.0.0:" + strconv.Itoa(*service_port))
  var listener, _ = net.ListenTCP("tcp", addr)

  for {
    var tcp_conn, _ = listener.AcceptTCP()
    go connectionHandler(tcp_conn)
  }
}


func connectionHandler(tcp_conn *net.TCPConn) {
  defer tcp_conn.Close()

  reader := bufio.NewReader(tcp_conn)

  for {
    line, _, _ := reader.ReadLine()
    tcp_conn.Write(line)
    fmt.Println(string(line))
  }
}
