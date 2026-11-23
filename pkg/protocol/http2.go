package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"golang.org/x/net/http2"
)

const (
	// MaxStreamID is the maximum valid stream ID in HTTP/2 (2^31 - 1).
	MaxStreamID uint32 = 0x7FFFFFFF

	// ErrCodeNoError indicates graceful shutdown according to RFC 7540.
	ErrCodeNoError http2.ErrCode = http2.ErrCodeNo
)

// HTTP2Drainer creates and manages HTTP/2 and gRPC graceful shutdown frames.
type HTTP2Drainer struct{}

// NewHTTP2Drainer creates an HTTP/2 drainer.
func NewHTTP2Drainer() *HTTP2Drainer {
	return &HTTP2Drainer{}
}

// BuildAdvisoryGoAwayFrame creates RFC 7540 Step 1 GOAWAY frame with stream ID 2^31 - 1.
// This informs the client that the connection is closing soon and to stop opening new streams,
// while letting existing in-flight streams continue without disruption.
func (d *HTTP2Drainer) BuildAdvisoryGoAwayFrame() []byte {
	return d.BuildGoAwayFrame(MaxStreamID, ErrCodeNoError, []byte("gracegate: draining quorum"))
}

// BuildFinalGoAwayFrame creates RFC 7540 Step 2 GOAWAY frame with the exact last processed stream ID.
func (d *HTTP2Drainer) BuildFinalGoAwayFrame(lastStreamID uint32) []byte {
	return d.BuildGoAwayFrame(lastStreamID, ErrCodeNoError, []byte("gracegate: drain complete"))
}

// BuildGoAwayFrame builds a binary GOAWAY frame using bytes buffer.
func (d *HTTP2Drainer) BuildGoAwayFrame(lastStreamID uint32, errCode http2.ErrCode, debugData []byte) []byte {
	var buf bytes.Buffer
	framer := http2.NewFramer(&buf, nil)

	// WriteGoAway(maxStreamID, errCode, debugData)
	_ = framer.WriteGoAway(lastStreamID, errCode, debugData)

	return buf.Bytes()
}

// ParseGoAwayFrame parses raw frame bytes into GOAWAY components.
func (d *HTTP2Drainer) ParseGoAwayFrame(raw []byte) (lastStreamID uint32, errCode http2.ErrCode, debugData []byte, err error) {
	reader := bytes.NewReader(raw)
	framer := http2.NewFramer(io.Discard, reader)

	frame, err := framer.ReadFrame()
	if err != nil {
		return 0, 0, nil, fmt.Errorf("failed to read HTTP/2 frame: %w", err)
	}

	goAway, ok := frame.(*http2.GoAwayFrame)
	if !ok {
		return 0, 0, nil, fmt.Errorf("expected GOAWAY frame, got %v", frame.Header().Type)
	}

	return goAway.LastStreamID, goAway.ErrCode, goAway.DebugData(), nil
}

// BuildRawFrameDirect provides a zero-dependency fallback binary frame builder.
func (d *HTTP2Drainer) BuildRawFrameDirect(lastStreamID uint32, errCode uint32, debugData []byte) []byte {
	payloadLen := 8 + len(debugData)
	totalLen := 9 + payloadLen
	frame := make([]byte, totalLen)

	// 24-bit Length
	frame[0] = byte(payloadLen >> 16)
	frame[1] = byte(payloadLen >> 8)
	frame[2] = byte(payloadLen)

	// Type = 0x7 (GOAWAY)
	frame[3] = 0x7

	// Flags = 0x0
	frame[4] = 0x0

	// Stream ID = 0 (Connection-level frame)
	frame[5] = 0x0
	frame[6] = 0x0
	frame[7] = 0x0
	frame[8] = 0x0

	// Payload: Last-Stream-ID (31-bit with reserved 1-bit set to 0)
	binary.BigEndian.PutUint32(frame[9:13], lastStreamID&0x7FFFFFFF)

	// Error Code (32-bit)
	binary.BigEndian.PutUint32(frame[13:17], errCode)

	// Debug Data
	if len(debugData) > 0 {
		copy(frame[17:], debugData)
	}

	return frame
}
