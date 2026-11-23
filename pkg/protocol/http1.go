package protocol

import (
	"bufio"
	"bytes"
	"fmt"
	"net/http"
	"strings"
)

// HTTP1Drainer handles HTTP/1.1 connection draining and keep-alive prevention.
type HTTP1Drainer struct{}

// NewHTTP1Drainer creates a drainer for HTTP/1.1 streams.
func NewHTTP1Drainer() *HTTP1Drainer {
	return &HTTP1Drainer{}
}

// MutateHeadersForDrain modifies headers so clients do not reuse keep-alive sockets.
func (d *HTTP1Drainer) MutateHeadersForDrain(h http.Header) {
	h.Set("Connection", "close")
	// Also remove Keep-Alive header if present
	h.Del("Keep-Alive")
}

// IsKeepAlive checks whether an HTTP/1.1 request specifies keep-alive.
func (d *HTTP1Drainer) IsKeepAlive(protoMajor, protoMinor int, header http.Header) bool {
	connHeader := strings.ToLower(header.Get("Connection"))
	if protoMajor == 1 && protoMinor == 1 {
		return !strings.Contains(connHeader, "close")
	}
	// HTTP/1.0 defaults to non-keep-alive unless requested
	return strings.Contains(connHeader, "keep-alive")
}

// InjectConnectionCloseRaw rewrites raw HTTP/1.1 header bytes to include Connection: close.
func (d *HTTP1Drainer) InjectConnectionCloseRaw(raw []byte) ([]byte, error) {
	reader := bufio.NewReader(bytes.NewReader(raw))
	resp, err := http.ReadResponse(reader, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to parse HTTP/1 response: %w", err)
	}
	defer resp.Body.Close()

	d.MutateHeadersForDrain(resp.Header)
	resp.Close = true

	var buf bytes.Buffer
	if err := resp.Write(&buf); err != nil {
		return nil, fmt.Errorf("failed to serialize modified response: %w", err)
	}

	return buf.Bytes(), nil
}
