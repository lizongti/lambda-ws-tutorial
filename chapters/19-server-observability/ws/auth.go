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

// Principal is the stable authenticated identity attached to connections and
// sessions. Claims contains only application-provided string claims.
type Principal struct {
	Subject string
	Claims  map[string]string
}

// Clone returns a Principal whose Claims map can be modified independently.
func (p Principal) Clone() Principal {
	clone := Principal{Subject: p.Subject}
	if p.Claims != nil {
		clone.Claims = make(map[string]string, len(p.Claims))
		for key, value := range p.Claims {
			clone.Claims[key] = value
		}
	}
	return clone
}

// String avoids accidentally logging claim values, which may contain secrets.
func (p Principal) String() string { return fmt.Sprintf("subject=%q", p.Subject) }

// GoString also redacts claims under the %#v formatting verb.
func (p Principal) GoString() string { return p.String() }

// ConnectRequest contains approved API Gateway connection metadata. The
// framework does not prescribe a token or credential format.
type ConnectRequest struct {
	ConnectionID                    string
	RequestID                       string
	Headers                         map[string]string
	QueryParameters                 map[string]string
	MultiValueHeaders               map[string][]string
	MultiValueQueryStringParameters map[string][]string
	RequestContext                  map[string]any
}

// String and GoString keep credentials in headers, query parameters and request
// context out of default formatting.
func (r ConnectRequest) String() string {
	return fmt.Sprintf("connectionId=%q requestId=%q", r.ConnectionID, r.RequestID)
}

func (r ConnectRequest) GoString() string { return r.String() }

// Action describes an operation to authorize. Resource is an optional opaque
// application resource; Path, RoomID and SessionID cover framework operations.
type Action struct {
	Name      string
	Resource  string
	Path      string
	RoomID    string
	SessionID string
}

type Authenticator interface {
	Authenticate(ctx context.Context, req ConnectRequest) (Principal, error)
}

type AuthenticatorFunc func(context.Context, ConnectRequest) (Principal, error)

func (f AuthenticatorFunc) Authenticate(ctx context.Context, req ConnectRequest) (Principal, error) {
	if f == nil {
		return Principal{}, errors.New("ws: authentication is not configured")
	}
	return f(ctx, req)
}

type Authorizer interface {
	Authorize(ctx context.Context, principal Principal, action Action) error
}

type AuthorizerFunc func(context.Context, Principal, Action) error

func (f AuthorizerFunc) Authorize(ctx context.Context, principal Principal, action Action) error {
	if f == nil {
		return errors.New("ws: authorization is not configured")
	}
	return f(ctx, principal, action)
}

// AnonymousAuthenticator is deliberately a distinct type so startup
// validation can reject it outside an explicit development configuration.
type AnonymousAuthenticator struct{}

func NewAnonymousAuthenticator() *AnonymousAuthenticator {
	return &AnonymousAuthenticator{}
}

func (*AnonymousAuthenticator) Authenticate(context.Context, ConnectRequest) (Principal, error) {
	return Principal{Subject: AnonymousSubject}, nil
}

func (*AnonymousAuthenticator) developmentAnonymous() {}

// AuthConfig contains the authentication choices needed for startup
// validation. The zero value is production-safe: Development is false.
type AuthConfig struct {
	Development bool
	Required    bool
	Protected   bool
	Anonymous   bool
}

// ValidateAuthConfig rejects missing production authentication and anonymous
// authentication which is either non-development or not explicitly selected.
func ValidateAuthConfig(config AuthConfig, authenticator Authenticator) error {
	_, anonymousAuthenticator := authenticator.(interface{ developmentAnonymous() })

	if !config.Development && (config.Anonymous || anonymousAuthenticator) {
		return ErrAnonymousNotAllowed
	}
	if config.Anonymous != anonymousAuthenticator {
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

// ValidateProductionAuth is a convenience helper for production startup.
func ValidateProductionAuth(authenticator Authenticator) error {
	return ValidateAuthConfig(AuthConfig{}, authenticator)
}

// RequireAuthorization authorizes the normalized request path before any
// later handler runs. Denials are converted to safe errors and abort the chain.
func RequireAuthorization(authorizer Authorizer) HandlerFunc {
	return func(ctx *Context) {
		if ctx == nil || ctx.IsAborted() || ctx.Err() != nil {
			return
		}
		if ctx.Principal.Subject == "" {
			ctx.AbortWithError(NewHandlerError("authentication_required", "authentication required"))
			return
		}
		if authorizer == nil {
			ctx.AbortWithError(NewHandlerError("internal_error", "authorization is not configured"))
			return
		}

		action := Action{
			Name:     ActionPathAccess,
			Resource: ctx.RoutePath(),
			Path:     ctx.RoutePath(),
		}
		if err := authorizer.Authorize(ctx.StdContext(), ctx.Principal.Clone(), action); err != nil {
			// Do not wrap or copy the application's error: it can contain policy
			// details, credentials or backend information.
			ctx.AbortWithError(NewHandlerError("authorization_denied", "access denied"))
		}
	}
}
