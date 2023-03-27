package tritonhttp

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

type Server struct {
	// Addr specifies the TCP address for the server to listen on,
	// in the form "host:port". It shall be passed to net.Listen()
	// during ListenAndServe().
	Addr string // e.g. ":0"

	// VirtualHosts contains a mapping from host name to the docRoot path
	// (i.e. the path to the directory to serve static files from) for
	// all virtual hosts that this server supports
	VirtualHosts map[string]string
}

// ListenAndServe listens on the TCP network address s.Addr and then
// handles requests on incoming connections.

func (s *Server) ListenAndServe() error {
	fmt.Println("Server setup valid!")

	// server should now start to listen on the configured address

	listener, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	defer func() {
		err = listener.Close()
		if err != nil {
			log.Println("Error closing the listener", err)
			return
		}
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		s.HandleConnection(conn)
	}
}

func (s *Server) HandleConnection(conn net.Conn) {
	lineReader := bufio.NewReader(conn)
	// not closing conn until timeout
	for {
		timeOutErr := conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if timeOutErr != nil {
			log.Println("Failed to set timeout for connection", conn)
			_ = conn.Close()
			return

		}
		request := &Request{}
		byteRead, requestErr := request.readRequest(lineReader)

		if requestErr != nil {

			if requestErr == io.EOF {
				log.Printf("Connection closed by %v", conn.RemoteAddr())
				_ = conn.Close()
				return
			}

			if err, ok := requestErr.(net.Error); ok && err.Timeout() {
				if byteRead {
					res := &Response{}
					res.HandleBadRequest()
					_ = res.write(conn)
					return
				}
				log.Printf("Connection to %v timed out", conn.RemoteAddr())
				_ = conn.Close()
				return
			} else {
				log.Printf("Handle bad request for error: %v", requestErr)
				res := &Response{}
				res.HandleBadRequest()
				_ = res.write(conn)
				_ = conn.Close()
				return
			}
		}

		log.Printf("Handle good request: %v", request)
		res := &Response{}
		res.HandleGoodRequest(request, s.VirtualHosts)
		wErr := res.write(conn)
		if wErr != nil {
			fmt.Println(wErr)
		}

		if request.Close {
			_ = conn.Close()
			return
		}
	}

}
