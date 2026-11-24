package ws

import (
	"context"
	"errors"
	"sync"

	wsstore "github.com/aura-studio/lambda/ws/store"
)

// BroadcastOptions controls generic fan-out. SenderSessionID and
// SenderConnectionID are trusted caller values, not fields read from payload.
// Principal is used for room.broadcast authorization; when empty, a sender
// session's stored principal subject is used.
type BroadcastOptions struct {
	ExcludeSender      bool
	SenderSessionID    string
	SenderConnectionID string
	Principal          Principal
	RequestID          string
}

// BroadcastDeliveryResult is the safe per-target outcome of a broadcast.
type BroadcastDeliveryResult struct {
	SessionID    string `json:"sessionId"`
	ConnectionID string `json:"connectionId"`
	MessageID    string `json:"messageId"`
	Status       string `json:"status"`
	ErrorCode    string `json:"errorCode,omitempty"`
	Gone         bool   `json:"gone,omitempty"`
	Offline      bool   `json:"offline,omitempty"`
}

// BroadcastResult includes aggregate counts and independent per-target results.
// Err reports a room-level failure such as authorization, pagination or context
// cancellation; individual delivery failures are represented in Deliveries.
type BroadcastResult struct {
	Total      int                       `json:"total"`
	Succeeded  int                       `json:"succeeded"`
	Gone       int                       `json:"gone"`
	Offline    int                       `json:"offline"`
	Failed     int                       `json:"failed"`
	Deliveries []BroadcastDeliveryResult `json:"deliveries,omitempty"`
	Err        error                     `json:"-"`
}

// JoinRoom authorizes and idempotently writes the current session binding.
func (e *Engine) JoinRoom(ctx *Context, roomID string) error {
	if e == nil || !e.Options.Rooms.Enabled {
		return ErrRoomsDisabled
	}
	if err := ValidateRoomID(roomID); err != nil {
		return err
	}
	_, session, connection, err := e.roomContextBinding(ctx)
	if err != nil {
		return err
	}
	if err := e.authorize(ctx.StdContext(), ctx.Principal, Action{Name: ActionRoomJoin, RoomID: roomID, SessionID: session.SessionID, Resource: roomID}); err != nil {
		return err
	}
	count, alreadyMember, err := e.roomMembershipCount(ctx.StdContext(), roomID, session.SessionID)
	if err != nil {
		return err
	}
	if count >= e.Options.Rooms.MaxMembers && !alreadyMember {
		return ErrRoomFull
	}
	member := wsstore.RoomMember{
		RoomID: roomID, SessionID: session.SessionID, ConnectionID: connection.ConnectionID,
		Generation: connection.Generation, JoinedAt: e.Options.Clock.Now(),
	}
	var joinErr error
	if bindingStore, ok := e.Options.StateStore.(wsstore.RoomBindingStore); ok {
		joinErr = bindingStore.JoinIfMatch(ctx.StdContext(), member)
	} else {
		joinErr = e.Options.StateStore.Join(ctx.StdContext(), member)
	}
	if joinErr != nil {
		if wsstore.IsQuotaExceeded(joinErr) {
			return errors.Join(ErrRoomFull, joinErr)
		}
		return joinErr
	}
	return nil
}

// LeaveRoom removes only the caller's membership and is idempotent.
func (e *Engine) LeaveRoom(ctx *Context, roomID string) error {
	if e == nil || !e.Options.Rooms.Enabled {
		return ErrRoomsDisabled
	}
	if err := ValidateRoomID(roomID); err != nil {
		return err
	}
	_, session, connection, err := e.roomContextBinding(ctx)
	if err != nil {
		return err
	}
	if err := e.authorize(ctx.StdContext(), ctx.Principal, Action{Name: ActionRoomLeave, RoomID: roomID, SessionID: session.SessionID, Resource: roomID}); err != nil {
		return err
	}
	if bindingStore, ok := e.Options.StateStore.(wsstore.RoomBindingStore); ok {
		return bindingStore.LeaveIfMatch(ctx.StdContext(), roomID, session.SessionID, connection.ConnectionID, connection.Generation)
	}
	return e.Options.StateStore.Leave(ctx.StdContext(), roomID, session.SessionID)
}

func (e *Engine) roomContextBinding(ctx *Context) (Principal, wsstore.Session, wsstore.Connection, error) {
	if ctx == nil || ctx.StdContext() == nil {
		return Principal{}, wsstore.Session{}, wsstore.Connection{}, ErrInvalidEvent
	}
	connection, session, err := e.validateActiveBinding(ctx.StdContext(), ctx.ConnectionID)
	if err != nil {
		return Principal{}, wsstore.Session{}, wsstore.Connection{}, err
	}
	if ctx.SessionID != session.SessionID || ctx.Principal.Subject != session.PrincipalSubject {
		return Principal{}, wsstore.Session{}, wsstore.Connection{}, ErrAuthorizationFailed
	}
	return ctx.Principal.Clone(), session, connection, nil
}

func (e *Engine) authorize(ctx context.Context, principal Principal, action Action) error {
	if nilDependency(e.Options.Authorizer) {
		return nil
	}
	if principal.Subject == "" {
		return ErrAuthorizationFailed
	}
	if err := e.Options.Authorizer.Authorize(ctx, principal.Clone(), action); err != nil {
		return ErrAuthorizationFailed
	}
	return nil
}

func (e *Engine) roomMembershipCount(ctx context.Context, roomID, sessionID string) (int, bool, error) {
	count := 0
	found := false
	cursor := ""
	for {
		members, next, err := e.Options.StateStore.Members(ctx, roomID, e.Options.Rooms.BroadcastBatchSize, cursor)
		if err != nil {
			return 0, false, err
		}
		for _, member := range members {
			count++
			if member.SessionID == sessionID {
				found = true
			}
		}
		if next == "" || len(members) == 0 {
			return count, found, nil
		}
		if next == cursor {
			return 0, false, ErrInternal
		}
		cursor = next
	}
}

// Broadcast performs bounded, paginated room fan-out. An empty room is a
// successful empty result and never causes implicit application room state.
func (e *Engine) Broadcast(ctx context.Context, roomID string, msg OutboundMessage, opts BroadcastOptions) BroadcastResult {
	var result BroadcastResult
	if e == nil || e.Options == nil || ctx == nil {
		result.Err = ErrInvalidEvent
		return result
	}
	if !e.Options.Rooms.Enabled {
		result.Err = ErrRoomsDisabled
		return result
	}
	if err := ValidateRoomID(roomID); err != nil {
		result.Err = err
		return result
	}
	// Validate the common envelope before reading any membership. Per-target
	// session and message IDs are added later and encoded again at the boundary.
	if _, err := EncodeOutboundMessageWithLimit(msg, e.Options.Message.MaxBytes); err != nil {
		result.Err = err
		return result
	}
	principal := opts.Principal.Clone()
	if principal.Subject == "" && opts.SenderSessionID != "" {
		if session, err := e.Options.StateStore.GetSession(ctx, opts.SenderSessionID); err == nil {
			principal.Subject = session.PrincipalSubject
		}
	}
	if err := e.authorize(ctx, principal, Action{Name: ActionRoomBroadcast, RoomID: roomID, SessionID: opts.SenderSessionID, Resource: roomID}); err != nil {
		result.Err = err
		e.observability.log(ctx, LogEvent{Name: LogEventBroadcast, Outcome: "denied", RequestID: opts.RequestID, RoomID: roomID, ErrorCode: "authorization_denied"})
		return result
	}

	cursor := ""
	for {
		members, next, err := e.Options.StateStore.Members(ctx, roomID, e.Options.Rooms.BroadcastBatchSize, cursor)
		if err != nil {
			result.Err = err
			return result
		}
		selected := make([]wsstore.RoomMember, 0, len(members))
		for _, member := range members {
			if opts.ExcludeSender && ((opts.SenderSessionID != "" && member.SessionID == opts.SenderSessionID) ||
				(opts.SenderConnectionID != "" && member.ConnectionID == opts.SenderConnectionID)) {
				continue
			}
			selected = append(selected, member)
		}
		pageResult := e.broadcastPage(ctx, msg, selected)
		mergeBroadcastResult(&result, pageResult)
		if ctx.Err() != nil {
			result.Err = ctx.Err()
			return result
		}
		if next == "" || len(members) == 0 {
			break
		}
		if next == cursor {
			result.Err = ErrInternal
			return result
		}
		cursor = next
	}
	e.observability.log(ctx, LogEvent{Name: LogEventBroadcast, Outcome: "success", RequestID: opts.RequestID, RoomID: roomID, Count: result.Succeeded + result.Offline})
	if result.Failed > 0 {
		e.observability.record(ctx, Metric{Name: MetricBroadcastFailures, Kind: MetricCounter, Value: int64(result.Failed), RequestID: opts.RequestID, RoomID: roomID})
	}
	return result
}

type roomDelivery struct {
	member wsstore.RoomMember
	result BroadcastDeliveryResult
	err    error
}

func (e *Engine) broadcastPage(ctx context.Context, msg OutboundMessage, members []wsstore.RoomMember) BroadcastResult {
	result := BroadcastResult{Total: len(members)}
	if len(members) == 0 {
		return result
	}
	workers := e.Options.Push.Concurrency
	if workers > len(members) {
		workers = len(members)
	}
	jobs := make(chan wsstore.RoomMember, len(members))
	completed := make(chan roomDelivery, len(members))
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for member := range jobs {
				deliveryMsg := msg
				if deliveryMsg.MessageID == "" {
					deliveryMsg.MessageID = e.Options.IDGenerator.ID("msg")
				}
				delivery, err := e.PushToSessionWithResult(ctx, member.SessionID, deliveryMsg)
				item := BroadcastDeliveryResult{
					SessionID: member.SessionID, ConnectionID: delivery.ConnectionID, MessageID: delivery.MessageID,
					Status: string(delivery.Status), Gone: delivery.Gone, Offline: delivery.Offline,
				}
				if err != nil {
					item.ErrorCode = safeErrorCode(err)
					if item.Status == "" {
						item.Status = "failed"
					}
				}
				completed <- roomDelivery{member: member, result: item, err: err}
			}
		}()
	}
	for _, member := range members {
		jobs <- member
	}
	close(jobs)
	wg.Wait()
	close(completed)
	for delivery := range completed {
		result.Deliveries = append(result.Deliveries, delivery.result)
		if delivery.result.Status == string(DeliverySent) {
			result.Succeeded++
		}
		if delivery.result.Gone {
			result.Gone++
		}
		if delivery.result.Offline {
			result.Offline++
		}
		if delivery.err != nil {
			result.Failed++
		}
	}
	return result
}

func mergeBroadcastResult(target *BroadcastResult, page BroadcastResult) {
	target.Total += page.Total
	target.Succeeded += page.Succeeded
	target.Gone += page.Gone
	target.Offline += page.Offline
	target.Failed += page.Failed
	target.Deliveries = append(target.Deliveries, page.Deliveries...)
}
