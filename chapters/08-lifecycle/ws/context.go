package ws

import (
	"context"
	"encoding/json"
	"errors"
)

// HandlerError is an error whose Code and Message are safe to return to a
// client. The wrapped cause is available to server-side code, but is never
// included in Error's output.
type HandlerError struct {
	Code      string
	Message   string
	Retryable bool
	cause     error
}

func NewHandlerError(code, message string) *HandlerError {
	return &HandlerError{Code: code, Message: message}
}

func WrapHandlerError(code, message string, cause error) *HandlerError {
	return &HandlerError{Code: code, Message: message, cause: cause}
}

func (e *HandlerError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message == "" {
		return e.Code
	}
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}

func (e *HandlerError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// GoString prevents a wrapped internal cause from appearing under %#v.
func (e *HandlerError) GoString() string { return e.Error() }

// HandlerErrorCode returns a stable code without exposing an arbitrary error's
// text. Errors which are not HandlerErrors are internal errors by default.
func HandlerErrorCode(err error) string {
	var handlerErr *HandlerError
	if errors.As(err, &handlerErr) && handlerErr.Code != "" {
		return handlerErr.Code
	}
	if err == nil {
		return ""
	}
	return "internal_error"
}

// Context contains the trusted transport identity and the current request.
// ConnectionID is the API Gateway connection, while SessionID is the logical
// session and may survive reconnection.
type Context struct {
	Context      context.Context
	ConnectionID string
	SessionID    string
	Principal    Principal
	Path         string
	RequestID    string
	MessageID    string
	Payload      json.RawMessage

	response                any
	responseSet             bool
	err                     error
	aborted                 bool
	routePath               string
	pathParam               string
	originatingConnectionID string
	originatingSessionID    string
}

// StdContext returns the invocation context, defaulting to Background for a
// Context constructed as a zero value.
func (c *Context) StdContext() context.Context {
	if c == nil || c.Context == nil {
		return context.Background()
	}
	return c.Context
}

// SetResponse records a response payload without sending it. Sending is
// deliberately left to Engine, which owns the Pusher.
func (c *Context) SetResponse(response any) {
	c.response = response
	c.responseSet = true
}

// AbortWithResponse records a response and stops the remaining handler chain.
func (c *Context) AbortWithResponse(response any) {
	c.SetResponse(response)
	c.Abort()
}

func (c *Context) Response() any {
	if c == nil {
		return nil
	}
	return c.response
}

func (c *Context) HasResponse() bool {
	return c != nil && c.responseSet
}

func (c *Context) SetError(err error) {
	c.err = err
}

func (c *Context) AbortWithError(err error) {
	c.SetError(err)
	c.Abort()
}

func (c *Context) Err() error {
	if c == nil {
		return nil
	}
	return c.err
}

// GetError follows the accessor naming used by the existing Lambda routers.
func (c *Context) GetError() error { return c.Err() }

func (c *Context) Abort() { c.aborted = true }

func (c *Context) IsAborted() bool { return c != nil && c.aborted }

// RoutePath is the normalized path used for matching. Path may contain the
// extracted *path parameter while a wildcard route's handlers are running.
func (c *Context) RoutePath() string {
	if c == nil {
		return ""
	}
	if c.routePath != "" {
		return c.routePath
	}
	return c.Path
}

// PathParam returns the value captured by a terminal *path route parameter.
func (c *Context) PathParam() string {
	if c == nil {
		return ""
	}
	return c.pathParam
}

// OriginatingConnectionID is captured at the dispatch boundary. Handlers may
// use it instead of a mutable message field when constructing push results.
func (c *Context) OriginatingConnectionID() string {
	if c == nil {
		return ""
	}
	if c.originatingConnectionID != "" {
		return c.originatingConnectionID
	}
	return c.ConnectionID
}

// OriginatingSessionID returns the immutable logical session captured at dispatch.
func (c *Context) OriginatingSessionID() string {
	if c == nil {
		return ""
	}
	if c.originatingSessionID != "" {
		return c.originatingSessionID
	}
	return c.SessionID
}
