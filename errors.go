package gomb

// custom errors

import (
	"errors"
)

// ErrInvalidBroker is returned when the broker is invalid.
var ErrInvalidBroker = errors.New("invalid broker")

// ErrMissingBroker is returned when the broker is missing.
var ErrMissingBroker = errors.New("missing broker")

// ErrInvalidBrokerOption is returned when the broker option is invalid.
var ErrInvalidBrokerOption = errors.New("invalid broker option")

// ErrInvalidBrokerOptionValue is returned when the broker option value is invalid.
var ErrInvalidBrokerOptionValue = errors.New("invalid broker option value")

// ErrInvalidBrokerOptions is returned when the broker options are invalid.
var ErrInvalidBrokerOptions = errors.New("invalid broker options")

// ErrTooManyAuths is returned when there are too many auths.
var ErrTooManyAuths = errors.New("too many auths are set - none or one is required")

// ErrConnection is returned when there is a connection error.
var ErrConnection = errors.New("connection error")

// ErrAuth is an auth setting related error.
type ErrAuth struct {
	Msg string
}

// Error returns the error message.
func (e *ErrAuth) Error() string {
	return e.Msg
}
