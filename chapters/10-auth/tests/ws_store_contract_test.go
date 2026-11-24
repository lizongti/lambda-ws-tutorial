package tests

import (
	"errors"
	wsstore "github.com/aura-studio/lambda/ws/store"
	"testing"
	"time"
)

func TestChapter03StoreModelsAndErrors(t *testing.T) {
	err := wsstore.NewError(wsstore.ErrorCodeConditionConflict, "BindConnection", errors.New("private backend detail"))
	if !wsstore.IsConditionConflict(err) || wsstore.IsRetryable(err) {
		t.Fatalf("wrong classification: %v", err)
	}
	if err.Error() == "" || err.Error() == "private backend detail" {
		t.Fatalf("unsafe error text: %q", err)
	}
	now := time.Now()
	session := wsstore.Session{SessionID: "s1", CurrentConnectionID: "c1", Generation: 2, LastActiveAt: now}
	connection := wsstore.Connection{ConnectionID: "c1", SessionID: session.SessionID, Generation: session.Generation}
	if connection.SessionID != session.SessionID || connection.Generation != 2 {
		t.Fatal("model binding mismatch")
	}
	var store wsstore.StateStore
	if store != nil {
		t.Fatal("nil interface must remain nil")
	}
}
