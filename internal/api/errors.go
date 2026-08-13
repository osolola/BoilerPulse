package api

// ErrorCode identifies a class of API error, per the spec's error taxonomy
// (§54). This type (and the ErrorBody/ErrorDetail envelope below) is
// exported so internal/gateway can speak the exact same error shape back to
// clients instead of inventing its own.
type ErrorCode string

const (
	ErrKeyNotFound       ErrorCode = "KEY_NOT_FOUND"
	ErrInvalidRequest    ErrorCode = "INVALID_REQUEST"
	ErrInternal          ErrorCode = "INTERNAL_ERROR"
	ErrLeaderUnavailable ErrorCode = "LEADER_UNAVAILABLE"
	ErrQuorumUnavailable ErrorCode = "QUORUM_UNAVAILABLE"
	ErrNodeUnavailable   ErrorCode = "NODE_UNAVAILABLE"
	ErrRateLimited       ErrorCode = "RATE_LIMITED"
	ErrUnauthorized      ErrorCode = "UNAUTHORIZED"
)

// ErrorBody is the JSON envelope returned on every non-2xx response.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail carries the machine-readable code and a human-readable message.
type ErrorDetail struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
}
