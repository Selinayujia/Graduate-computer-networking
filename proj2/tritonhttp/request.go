package tritonhttp

import (
	"bufio"
	"fmt"
	"path/filepath"
	"strings"
)

type Request struct {
	Method string // e.g. "GET"
	URL    string // e.g. "/path/to/a/file"
	Proto  string // e.g. "HTTP/1.1"

	// Headers stores the key-value HTTP headers
	Headers map[string]string

	Host  string // determine from the "Host" header
	Close bool   // determine from the "Connection" header
}

func (request *Request) readRequest(lineReader *bufio.Reader) (byteRead bool, err error) {

	starterLine, err := lineReader.ReadString('\n')
	if err != nil {
		request = nil
		return false, err
	}
	starterLine = strings.TrimSuffix(starterLine, "\r\n")

	err = request.parseRequestStarterLine(starterLine)
	if err != nil {
		request = nil
		return false, err
	}

	request.Headers = make(map[string]string)
	for {
		headerLine, err := lineReader.ReadString('\n')
		if err != nil {
			request = nil
			return true, err
		}
		headerLine = strings.TrimSuffix(headerLine, "\r\n")

		if headerLine == "" {
			break
		} else {
			delim := ": "
			keyValue := strings.SplitN(headerLine, delim, 2)
			if len(keyValue) != 2 {
				request = nil
				return true, fmt.Errorf("400 request header malformat")
			}
			key := CanonicalHeaderKey(keyValue[0])
			value := strings.TrimSpace(keyValue[1])
			request.Headers[key] = value
		}

	}
	hasHost := request.handleSpecialHeaders()
	if !hasHost {
		request = nil
		return true, fmt.Errorf("400 request without host")
	}
	return true, nil
}

func (r *Request) parseRequestStarterLine(line string) (err error) {
	parts := strings.SplitN(line, " ", 3)
	if len(parts) != 3 {
		return fmt.Errorf("400 bad request starter line")
	}
	r.Method = parts[0]

	if r.Method != "GET" {
		return fmt.Errorf("400 request method not allowed")
	}

	r.URL = filepath.Clean(parts[1])
	fmt.Println("the url is", (r.URL))
	if r.URL[0] != '/' {
		return fmt.Errorf("400 bad url")
	}

	if strings.HasSuffix(r.URL, "/") {
		r.URL = r.URL + "index.html"
	}

	if parts[2] != "HTTP/1.1" {
		return fmt.Errorf("400 invalid proto")
	}
	r.Proto = parts[2]
	return nil
}

func (r *Request) handleSpecialHeaders() (hasHost bool) {

	hasHost = true
	host, ok := r.Headers["Host"]
	if ok {
		r.Host = host
	} else {
		hasHost = false
	}

	connection, ok := r.Headers["Connection"]
	if ok && connection == "close" {
		r.Close = true
	} else {
		r.Close = false
	}
	return hasHost
}
