package tests

import (
	"context"
	"errors"
	"github.com/aura-studio/lambda/ws"
	"testing"
)

func TestChapter06FakePusherAndErrors(t *testing.T) {
	gone := ws.NewPushError(ws.PushErrorGone, errors.New("410"))
	temporary := ws.NewPushError(ws.PushErrorThrottled, errors.New("limited"))
	if ws.IsRetryablePushError(gone) || !ws.IsPushGone(gone) || !ws.IsRetryablePushError(temporary) {
		t.Fatal("push error classification mismatch")
	}
	pusher := ws.NewFakePusher(nil)
	if err := pusher.PostToConnection(context.Background(), "c1", []byte(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	calls := pusher.Calls()
	if len(calls) != 1 || calls[0].ConnectionID != "c1" || string(calls[0].Body) != `{"ok":true}` {
		t.Fatalf("calls=%#v", calls)
	}
	calls[0].Body[0] = 'X'
	if string(pusher.Calls()[0].Body)[0] == 'X' {
		t.Fatal("recorded body aliases caller mutation")
	}
}
