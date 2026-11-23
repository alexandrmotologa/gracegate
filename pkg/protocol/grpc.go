package protocol

import (
	"net/http"
	"strconv"
	"strings"
)

// GRPCStatusCode represents standard gRPC status codes.
type GRPCStatusCode int

const (
	GRPCStatusOk                 GRPCStatusCode = 0
	GRPCStatusCanceled           GRPCStatusCode = 1
	GRPCStatusUnknown            GRPCStatusCode = 2
	GRPCStatusInvalidArgument    GRPCStatusCode = 3
	GRPCStatusDeadlineExceeded   GRPCStatusCode = 4
	GRPCStatusNotFound           GRPCStatusCode = 5
	GRPCStatusAlreadyExists      GRPCStatusCode = 6
	GRPCStatusPermissionDenied   GRPCStatusCode = 7
	GRPCStatusResourceExhausted  GRPCStatusCode = 8
	GRPCStatusFailedPrecondition GRPCStatusCode = 9
	GRPCStatusAborted            GRPCStatusCode = 10
	GRPCStatusOutOfRange         GRPCStatusCode = 11
	GRPCStatusUnimplemented      GRPCStatusCode = 12
	GRPCStatusInternal           GRPCStatusCode = 13
	GRPCStatusUnavailable        GRPCStatusCode = 14
	GRPCStatusDataLoss           GRPCStatusCode = 15
	GRPCStatusUnauthenticated    GRPCStatusCode = 16
)

// GRPCDrainer provides gRPC-specific inspection and draining helpers.
type GRPCDrainer struct{}

// NewGRPCDrainer creates a drainer for gRPC channels.
func NewGRPCDrainer() *GRPCDrainer {
	return &GRPCDrainer{}
}

// IsGRPCRequest checks if a request has gRPC content type.
func (g *GRPCDrainer) IsGRPCRequest(r *http.Request) bool {
	ct := r.Header.Get("Content-Type")
	return strings.HasPrefix(ct, "application/grpc")
}

// ExtractGRPCStatus retrieves the gRPC status from response trailers or headers.
func (g *GRPCDrainer) ExtractGRPCStatus(header, trailer http.Header) (GRPCStatusCode, string) {
	statusStr := trailer.Get("grpc-status")
	if statusStr == "" {
		statusStr = header.Get("grpc-status")
	}

	if statusStr == "" {
		return GRPCStatusOk, ""
	}

	code, err := strconv.Atoi(statusStr)
	if err != nil {
		return GRPCStatusUnknown, "invalid status code"
	}

	msg := trailer.Get("grpc-message")
	if msg == "" {
		msg = header.Get("grpc-message")
	}

	return GRPCStatusCode(code), msg
}

// IsZeroLossSafe checks if the RPC completed without being dropped.
func (g *GRPCDrainer) IsZeroLossSafe(code GRPCStatusCode) bool {
	switch code {
	case GRPCStatusUnavailable, GRPCStatusCanceled:
		return false
	default:
		return true
	}
}
