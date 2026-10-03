package alarm

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/journal"
	"github.com/wyw14/cry-98/internal/model"
)

type fakeDispatcher struct {
	results []model.ChannelResult
	err     error
}

func (f *fakeDispatcher) Dispatch(_ context.Context, _ model.Alarm) ([]model.ChannelResult, error) {
	return f.results, f.err
}

type fakeScheduler struct {
	mu      sync.Mutex
	pending []model.Alarm
}

func (f *fakeScheduler) Schedule(alarm model.Alarm) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pending = append(f.pending, alarm)
}

type harness struct {
	store    *journal.Store
	service  *Service
	dispatch *fakeDispatcher
}

func newHarness(t *testing.T, root string) *harness {
	t.Helper()
	now := func() time.Time { return time.Unix(1700000000, 0).UTC() }
	store, err := journal.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	dispatch := &fakeDispatcher{results: []model.ChannelResult{
		{Channel: "siren", Required: true, Delivered: true, Attempted: now()},
	}}
	delivery, err := NewDeliveryService(dispatch, &fakeScheduler{}, store, now)
	if err != nil {
		t.Fatal(err)
	}
	return &harness{store: store, service: NewService(delivery, store, now), dispatch: dispatch}
}

func (h *harness) raiseDelivered(t *testing.T) model.Alarm {
	t.Helper()
	alarm, err := h.service.Raise(context.Background(), "V-03", model.AlarmLeak, "leak detected")
	if err != nil {
		t.Fatalf("raise: %v", err)
	}
	if alarm.State != model.AlarmDelivered {
		t.Fatalf("state after raise = %s, want delivered", alarm.State)
	}
	return alarm
}

func TestAcknowledgeAndClearPersistAcrossRestart(t *testing.T) {
	root := t.TempDir()
	h := newHarness(t, root)
	alarm := h.raiseDelivered(t)

	ack, err := h.service.Acknowledge(alarm.ID)
	if err != nil {
		t.Fatalf("acknowledge: %v", err)
	}
	if ack.State != model.AlarmAcknowledged {
		t.Fatalf("state = %s", ack.State)
	}

	// Rebuild the service from the same journal, as a process restart would.
	restored := newHarness(t, root)
	if err := NewRecovery(restored.store, restored.service).Restore(); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, ok := restored.service.Get(alarm.ID)
	if !ok {
		t.Fatal("alarm missing after restart")
	}
	if got.State != model.AlarmAcknowledged {
		t.Fatalf("state after restart = %s, want acknowledged", got.State)
	}

	cleared, err := restored.service.Clear(alarm.ID)
	if err != nil {
		t.Fatalf("clear: %v", err)
	}
	if cleared.State != model.AlarmCleared {
		t.Fatalf("state = %s", cleared.State)
	}

	// A second restart must keep the cleared state rather than reverting to
	// the last delivery record.
	restoredAgain := newHarness(t, root)
	if err := NewRecovery(restoredAgain.store, restoredAgain.service).Restore(); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, ok = restoredAgain.service.Get(alarm.ID)
	if !ok {
		t.Fatal("alarm missing after second restart")
	}
	if got.State != model.AlarmCleared {
		t.Fatalf("state after second restart = %s, want cleared", got.State)
	}
}

func TestPersistenceFailureDoesNotChangeInMemoryState(t *testing.T) {
	root := t.TempDir()
	h := newHarness(t, root)
	alarm := h.raiseDelivered(t)

	// Make further appends fail: replacing the partition file with a
	// directory makes the open fail with EISDIR for every user.
	if err := removePartitionFileForFailure(root); err != nil {
		t.Fatal(err)
	}

	if _, err := h.service.Acknowledge(alarm.ID); err == nil {
		t.Fatal("expected persistence error, got nil")
	}
	got, ok := h.service.Get(alarm.ID)
	if !ok {
		t.Fatal("alarm disappeared after failed write")
	}
	if got.State != model.AlarmDelivered {
		t.Fatalf("state after failed write = %s, want delivered (no phantom success)", got.State)
	}

	// A failed clear must behave identically.
	if _, err := h.service.Clear(alarm.ID); err == nil {
		t.Fatal("expected persistence error, got nil")
	}
	got, _ = h.service.Get(alarm.ID)
	if got.State != model.AlarmDelivered {
		t.Fatalf("state after failed clear = %s, want delivered", got.State)
	}
}

func TestConcurrentDuplicateAcknowledgeJournalsOnce(t *testing.T) {
	root := t.TempDir()
	h := newHarness(t, root)
	alarm := h.raiseDelivered(t)

	const goroutines = 32
	var wg sync.WaitGroup
	results := make(chan model.Alarm, goroutines)
	errs := make(chan error, goroutines)
	start := make(chan struct{})
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			value, err := h.service.Acknowledge(alarm.ID)
			if err != nil {
				errs <- err
				return
			}
			results <- value
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		t.Fatalf("duplicate acknowledge failed: %v", err)
	}
	count := 0
	for value := range results {
		count++
		if value.State != model.AlarmAcknowledged {
			t.Fatalf("duplicate returned state %s", value.State)
		}
	}
	if count != goroutines {
		t.Fatalf("successful responses = %d, want %d", count, goroutines)
	}

	events, err := h.store.Events("alarms", 0)
	if err != nil {
		t.Fatal(err)
	}
	ackEvents := 0
	for _, event := range events {
		if event.Kind == "alarm.acknowledged" {
			ackEvents++
		}
	}
	if ackEvents != 1 {
		t.Fatalf("acknowledged events journaled = %d, want exactly 1", ackEvents)
	}
}

func TestTransitionRulesAndIdempotency(t *testing.T) {
	root := t.TempDir()
	h := newHarness(t, root)
	alarm := h.raiseDelivered(t)

	// Duplicate clear on an already cleared alarm is an idempotent success.
	if _, err := h.service.Clear(alarm.ID); err != nil {
		t.Fatalf("clear: %v", err)
	}
	again, err := h.service.Clear(alarm.ID)
	if err != nil {
		t.Fatalf("duplicate clear: %v", err)
	}
	if again.State != model.AlarmCleared {
		t.Fatalf("duplicate clear state = %s", again.State)
	}

	// Acknowledging a terminal (cleared) alarm is rejected.
	if _, err := h.service.Acknowledge(alarm.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("ack after clear err = %v, want ErrInvalidTransition", err)
	}

	// Unknown alarms are not-found rather than invalid-transition.
	if _, err := h.service.Clear(uuid.New()); !errors.Is(err, ErrAlarmNotFound) {
		t.Fatalf("clear unknown err = %v, want ErrAlarmNotFound", err)
	}

	events, err := h.store.Events("alarms", 0)
	if err != nil {
		t.Fatal(err)
	}
	clearEvents := 0
	for _, event := range events {
		if event.Kind == "alarm.cleared" {
			clearEvents++
		}
	}
	if clearEvents != 1 {
		t.Fatalf("cleared events journaled = %d, want exactly 1", clearEvents)
	}
}

func TestAcknowledgeThenClearPersistsBothTransitions(t *testing.T) {
	root := t.TempDir()
	h := newHarness(t, root)
	alarm := h.raiseDelivered(t)
	if _, err := h.service.Acknowledge(alarm.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.service.Clear(alarm.ID); err != nil {
		t.Fatal(err)
	}

	restored := newHarness(t, root)
	if err := NewRecovery(restored.store, restored.service).Restore(); err != nil {
		t.Fatal(err)
	}
	got, ok := restored.service.Get(alarm.ID)
	if !ok || got.State != model.AlarmCleared {
		t.Fatalf("after restart: ok=%v state=%s, want cleared", ok, got.State)
	}
}

func removePartitionFileForFailure(root string) error {
	path := filepath.Join(root, "alarms.events.jsonl")
	if err := os.Remove(path); err != nil {
		return err
	}
	return os.Mkdir(path, 0o755)
}
