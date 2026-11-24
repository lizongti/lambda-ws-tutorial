package ws

import (
	"context"
	"errors"
	"fmt"
)

const (
	ActionPathAccess    = "path.access"
	ActionRoomJoin      = "room.join"
	ActionRoomLeave     = "room.leave"
	ActionRoomBroadcast = "room.broadcast"
	ActionSessionResume = "session.resume"
	AnonymousSubject    = "anonymous"
)

var (
	ErrAuthenticatorRequired = errors.New("ws: authenticator required")
	ErrAnonymousNotAllowed   = errors.New("ws: anonymous authentication is development-only")
	ErrAnonymousNotExplicit  = errors.New("ws: anonymous authentication must be explicitly configured")
)

type Principal struct {
	Subject string
	Claims  map[string]string
}

func (p Principal) Clone() Principal {
	clone := Principal{Subject: p.Subject}
	if p.Claims != nil {
		clone.Claims = map[string]string{}
		for k, v := range p.Claims {
			clone.Claims[k] = v
		}
	}
	return clone
}
func (p Principal) String() string   { return fmt.Sprintf("subject=%q", p.Subject) }
func (p Principal) GoString() string { return p.String() }

type ConnectRequest struct {
	ConnectionID                    string
	RequestID                       string
	Headers                         map[string]string
	QueryParameters                 map[string]string
	MultiValueHeaders               map[string][]string
	MultiValueQueryStringParameters map[string][]string
	RequestContext                  map[string]any
}
type Action struct{ Name, Resource, Path, RoomID, SessionID string }
type Authenticator interface {
	Authenticate(context.Context, ConnectRequest) (Principal, error)
}
type AuthenticatorFunc func(context.Context, ConnectRequest) (Principal, error)

func (f AuthenticatorFunc) Authenticate(ctx context.Context, req ConnectRequest) (Principal, error) {
	return f(ctx, req)
}

type Authorizer interface {
	Authorize(context.Context, Principal, Action) error
}
type AuthorizerFunc func(context.Context, Principal, Action) error

func (f AuthorizerFunc) Authorize(ctx context.Context, p Principal, a Action) error {
	return f(ctx, p, a)
}

type AnonymousAuthenticator struct{}

func NewAnonymousAuthenticator() *AnonymousAuthenticator { return &AnonymousAuthenticator{} }
func (*AnonymousAuthenticator) Authenticate(context.Context, ConnectRequest) (Principal, error) {
	return Principal{Subject: AnonymousSubject}, nil
}
func (*AnonymousAuthenticator) developmentAnonymous() {}

type AuthConfig struct{ Development, Required, Protected, Anonymous bool }

func ValidateAuthConfig(config AuthConfig, authenticator Authenticator) error {
	_, anonymous := authenticator.(interface{ developmentAnonymous() })
	if !config.Development && (config.Anonymous || anonymous) {
		return ErrAnonymousNotAllowed
	}
	if config.Anonymous != anonymous {
		return ErrAnonymousNotExplicit
	}
	if !config.Development && authenticator == nil {
		return ErrAuthenticatorRequired
	}
	if (config.Required || config.Protected) && authenticator == nil {
		return ErrAuthenticatorRequired
	}
	return nil
}
func ValidateProductionAuth(authenticator Authenticator) error {
	return ValidateAuthConfig(AuthConfig{}, authenticator)
}
