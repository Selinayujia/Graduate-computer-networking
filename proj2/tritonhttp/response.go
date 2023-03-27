package tritonhttp

import (
	"errors"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

type Response struct {
	Proto      string // e.g. "HTTP/1.1"
	StatusCode int    // e.g. 200
	StatusText string // e.g. "OK"

	// Headers stores all headers to write to the response.
	Headers map[string]string

	// Request is the valid request that leads to this response.
	// It could be nil for responses not resulting from a valid request.
	// Hint: you might need this to handle the "Connection: Close" requirement
	Request *Request

	// FilePath is the local path to the file to serve.
	// It could be "", which means there is no file to serve.
	FilePath string
}

func (res *Response) HandleGoodRequest(request *Request, virtualHosts map[string]string) {
	found := false
	for host, docRoot := range virtualHosts {
		if request.Host == host {
			absFilePath := filepath.Join(docRoot, request.URL)
			if absFilePath[:len(docRoot)] != docRoot {
				break
			} else if _, err := os.Stat(absFilePath); errors.Is(err, os.ErrNotExist) {
				break
			} else {
				found = true
				res.HandleOK(request, absFilePath)
			}

		}

	}
	if !found {
		res.HandleNotFound(request)
	}
}

func (res *Response) init(request *Request) {
	res.Proto = "HTTP/1.1"
	res.Headers = make(map[string]string)
	res.Request = request
	res.FilePath = ""
	res.Headers["Date"] = FormatTime(time.Now())
	if request != nil {
		if request.Close {
			res.Headers["Connection"] = "close"
		}
	}
}

func (res *Response) HandleNotFound(request *Request) {
	res.init(request)
	res.StatusCode = 404
	res.StatusText = "Not Found"
}

func (res *Response) HandleBadRequest() {
	res.init(nil)
	res.StatusCode = 400
	res.StatusText = "Bad Request"
	res.Headers["Connection"] = "close"
}

// HandleOK prepares res to be a 200 OK response
// ready to be written back to client.
func (res *Response) HandleOK(request *Request, filePath string) {
	res.init(request)
	res.StatusCode = 200
	res.StatusText = "OK"
	res.FilePath = filePath

	stats, err := os.Stat(filePath)
	if errors.Is(err, os.ErrNotExist) {
		log.Print(err)
	}
	res.Headers["Last-Modified"] = FormatTime(stats.ModTime())
	res.Headers["Content-Length"] = strconv.FormatInt(stats.Size(), 10)
	res.Headers["Content-Type"] = MIMETypeByExtension(filepath.Ext(filePath))
}

func (res *Response) write(conn net.Conn) (err error) {
	err = res.writeResStatusLine(conn)
	if err != nil {
		return err
	}
	err = res.writeResHeaders(conn)
	if err != nil {
		return err
	}
	err = res.writeResBody(conn)
	return err

}

func (res *Response) writeResStatusLine(conn net.Conn) (err error) {
	statusLine := res.Proto + " " + strconv.Itoa(res.StatusCode) + " " + res.StatusText + "\r\n"
	_, err = conn.Write([]byte(statusLine))
	return err
}

func (res *Response) writeResHeaders(conn net.Conn) (err error) {
	sortedKeys := make([]string, 0, len(res.Headers))

	for key := range res.Headers {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)

	for _, key := range sortedKeys {
		header := key + ": " + res.Headers[key] + "\r\n"
		if _, err := conn.Write([]byte(header)); err != nil {
			return err
		}
	}
	_, err = conn.Write([]byte("\r\n"))

	return err
}

func (res *Response) writeResBody(conn net.Conn) (err error) {
	var body []byte
	if res.FilePath != "" {
		body, err = os.ReadFile(res.FilePath)
		if err != nil {
			return err
		}
	}
	_, err = conn.Write(body)
	return err
}
