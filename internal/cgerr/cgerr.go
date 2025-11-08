package cgerr

import "github.com/cockroachdb/errors"

const (
	CodeResource   = "resource"
	CodeTool       = "tool"
	CodeAuth       = "auth"
	CodeConfig     = "config"
	CodeRPC        = "rpc"
	CodeRTM        = "rtm"
	CodeValidation = "validation"
	CodeInternal   = "internal"
)

type Error struct {
	err error
}

func (e *Error) Error() string {
	if e == nil || e.err == nil {
		return ""
	}
	return e.err.Error()
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return errors.UnwrapOnce(e.err)
}

func (e *Error) WithDetail(key string, value any) *Error {
	if e == nil {
		return nil
	}
	e.err = errors.WithDetailf(e.err, "%s=%v", key, value)
	return e
}

func (e *Error) WithDetailf(key, format string, args ...any) *Error {
	if e == nil {
		return nil
	}
	combinedArgs := append([]any{key}, args...)
	e.err = errors.WithDetailf(e.err, "%s="+format, combinedArgs...)
	return e
}

func wrapWithCode(code, message string, cause error) *Error {
	var err error
	if cause != nil {
		err = errors.Wrap(cause, message)
	} else {
		err = errors.New(message)
	}
	err = errors.WithDetailf(err, "code=%s", code)
	return &Error{err: err}
}

func NewResourceError(cause error, message string) *Error {
	return wrapWithCode(CodeResource, message, cause)
}

func NewToolError(cause error, message string) *Error {
	return wrapWithCode(CodeTool, message, cause)
}

func NewAuthError(cause error, message string) *Error {
	return wrapWithCode(CodeAuth, message, cause)
}

func NewConfigError(cause error, message string) *Error {
	return wrapWithCode(CodeConfig, message, cause)
}

func NewRPCError(cause error, message string) *Error {
	return wrapWithCode(CodeRPC, message, cause)
}

func NewRTMError(cause error, message string) *Error {
	return wrapWithCode(CodeRTM, message, cause)
}

func NewValidationError(cause error, message string) *Error {
	return wrapWithCode(CodeValidation, message, cause)
}

func NewInternalError(cause error, message string) *Error {
	return wrapWithCode(CodeInternal, message, cause)
}

func Is(err error, target *Error) bool {
	if err == nil || target == nil {
		return false
	}
	return errors.Is(err, target)
}
