package alarm

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/journal"
	"github.com/wyw14/cry-98/internal/model"
)

type stubDispatcher struct {
	results []model.ChannelResult
	err     error
}

func (d stubDispatcher) Dispatch(context.Context, model.Alarm) ([]model.ChannelResult, error) {
	return d.results, d.err
}

type stubRetries struct {
	mu        sync.Mutex
	scheduled []model.Alarm
}

func (r *stubRetries) Schedule(alarm model.Alarm) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.scheduled = append(r.scheduled, alarm)
}

type failingAppender struct{ err error }

func (a failingAppender) Append(model.Event) (model.Event, error) {
	return model.Event{}, a.err
}

func deliveredAlarm(t *testing.T, now time.Time) model.Alarm {
	t.Helper()
	alarm, err := model.NewAlarm("V-03", model.AlarmPressure, "pressure above threshold", now)
	if err != nil {
		t.Fatal(err)
	}
	return alarm.WithChannels([]model.ChannelResult{
		{Channel: "siren", Required: true, Delivered: true, Attempted: now},
	}, now)
}

// restart opens a fresh service over the same journal directory and replays
// it, simulating a process restart.
func restart(t *testing.T, root string) *Service {
	t.Helper()
	store, err := journal.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(nil, store, nil)
	if err := NewRecovery(store, service).Restore(); err != nil {
		t.Fatal(err)
	}
	return service
}

func stateEvents(t *testing.T, store *journal.Store) []model.Event {
	t.Helper()
	events, err := store.Events(eventPartition, 0)
	if err != nil {
		t.Fatal(err)
	}
	result := make([]model.Event, 0)
	for _, event := range events {
		if event.Kind == eventKindState {
			result = append(result, event)
		}
	}
	return result
}

func TestAcknowledgeSurvivesRestart(t *testing.T) {
	root := t.TempDir()
	store, err := journal.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(nil, store, nil)
	alarm := deliveredAlarm(t, time.Unix(1000, 0))
	service.Restore(alarm)

	acknowledged, err := service.Acknowledge(alarm.ID)
	if err != nil {
		t.Fatal(err)
	}
	if acknowledged.State != model.AlarmAcknowledged {
		t.Fatalf("state=%s", acknowledged.State)
	}

	restored := restart(t, root)
	replayed, ok := restored.Get(alarm.ID)
	if !ok {
		t.Fatal("alarm missing after restart")
	}
	if replayed.State != model.AlarmAcknowledged {
		t.Fatalf("state after restart=%s", replayed.State)
	}
	if !replayed.UpdatedAt.Equal(acknowledged.UpdatedAt) {
		t.Fatalf("updated_at changed across restart: %s != %s", replayed.UpdatedAt, acknowledged.UpdatedAt)
	}
}

func TestClearSurvivesRestart(t *testing.T) {
	root := t.TempDir()
	store, err := journal.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(nil, store, nil)
	alarm := deliveredAlarm(t, time.Unix(1000, 0))
	service.Restore(alarm)

	if _, err := service.Acknowledge(alarm.ID); err != nil {
		t.Fatal(err)
	}
	cleared, err := service.Clear(alarm.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cleared.State != model.AlarmCleared {
		t.Fatalf("state=%s", cleared.State)
	}

	restored := restart(t, root)
	replayed, ok := restored.Get(alarm.ID)
	if !ok {
		t.Fatal("alarm missing after restart")
	}
	if replayed.State != model.AlarmCleared {
		t.Fatalf("state after restart=%s", replayed.State)
	}
}

func TestDuplicateAcknowledgeIsIdempotent(t *testing.T) {
	root := t.TempDir()
	store, err := journal.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(nil, store, nil)
	alarm := deliveredAlarm(t, time.Unix(1000, 0))
	service.Restore(alarm)

	first, err := service.Acknowledge(alarm.ID)
	if err != nil {
		t.Fatal(err)
	}
	again, err := service.Acknowledge(alarm.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !again.UpdatedAt.Equal(first.UpdatedAt) {
		t.Fatalf("duplicate acknowledge mutated state: %s != %s", again.UpdatedAt, first.UpdatedAt)
	}
	if events := stateEvents(t, store); len(events) != 1 {
		t.Fatalf("duplicate acknowledge journaled extra events: %d", len(events))
	}

	restored := restart(t, root)
	replayed, _ := restored.Get(alarm.ID)
	if replayed.State != model.AlarmAcknowledged || !replayed.UpdatedAt.Equal(first.UpdatedAt) {
		t.Fatalf("restart diverged: %+v", replayed)
	}
}

func TestDuplicateClearIsIdempotent(t *testing.T) {
	root := t.TempDir()
	store, err := journal.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(nil, store, nil)
	alarm := deliveredAlarm(t, time.Unix(1000, 0))
	service.Restore(alarm)

	first, err := service.Clear(alarm.ID)
	if err != nil {
		t.Fatal(err)
	}
	again, err := service.Clear(alarm.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !again.UpdatedAt.Equal(first.UpdatedAt) {
		t.Fatalf("duplicate clear mutated state: %s != %s", again.UpdatedAt, first.UpdatedAt)
	}
	if events := stateEvents(t, store); len(events) != 1 {
		t.Fatalf("duplicate clear journaled extra events: %d", len(events))
	}
}

func TestAcknowledgeAfterClearConflicts(t *testing.T) {
	root := t.TempDir()
	store, err := journal.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(nil, store, nil)
	alarm := deliveredAlarm(t, time.Unix(1000, 0))
	service.Restore(alarm)

	if _, err := service.Clear(alarm.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Acknowledge(alarm.ID); !errors.Is(err, ErrAlarmStateConflict) {
		t.Fatalf("err=%v", err)
	}
	current, _ := service.Get(alarm.ID)
	if current.State != model.AlarmCleared {
		t.Fatalf("state=%s", current.State)
	}

	restored := restart(t, root)
	replayed, _ := restored.Get(alarm.ID)
	if replayed.State != model.AlarmCleared {
		t.Fatalf("state after restart=%s", replayed.State)
	}
}

func TestTransitionUnknownAlarm(t *testing.T) {
	root := t.TempDir()
	store, err := journal.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(nil, store, nil)
	if _, err := service.Acknowledge(uuid.New()); !errors.Is(err, ErrAlarmNotFound) {
		t.Fatalf("acknowledge err=%v", err)
	}
	if _, err := service.Clear(uuid.New()); !errors.Is(err, ErrAlarmNotFound) {
		t.Fatalf("clear err=%v", err)
	}
}

func TestTransitionFailsWhenJournalAppendFails(t *testing.T) {
	appendErr := errors.New("disk full")
	service := NewService(nil, failingAppender{err: appendErr}, nil)
	alarm := deliveredAlarm(t, time.Unix(1000, 0))
	service.Restore(alarm)

	if _, err := service.Acknowledge(alarm.ID); !errors.Is(err, ErrAlarmPersistence) {
		t.Fatalf("acknowledge err=%v", err)
	}
	if _, err := service.Clear(alarm.ID); !errors.Is(err, ErrAlarmPersistence) {
		t.Fatalf("clear err=%v", err)
	}
	current, ok := service.Get(alarm.ID)
	if !ok {
		t.Fatal("alarm missing")
	}
	if current.State != model.AlarmDelivered {
		t.Fatalf("in-memory state changed despite persistence failure: %s", current.State)
	}
}

func TestRaiseFailsWhenDeliveryCannotBeJournaled(t *testing.T) {
	appendErr := errors.New("journal unavailable")
	delivery, err := NewDeliveryService(
		stubDispatcher{results: []model.ChannelResult{{Channel: "siren", Required: true, Delivered: true}}},
		&stubRetries{},
		failingAppender{err: appendErr},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(delivery, failingAppender{err: appendErr}, nil)

	raised, err := service.Raise(context.Background(), "V-03", model.AlarmLeak, "nitrogen leak")
	if !errors.Is(err, ErrAlarmPersistence) {
		t.Fatalf("err=%v", err)
	}
	if raised.ID != uuid.Nil {
		t.Fatalf("reported alarm id %s despite persistence failure", raised.ID)
	}
	if items := service.List(); len(items) != 0 {
		t.Fatalf("alarm visible despite persistence failure: %v", items)
	}
}

func TestRaisePersistsDeliveryAndSurvivesRestart(t *testing.T) {
	root := t.TempDir()
	store, err := journal.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := NewDeliveryService(
		stubDispatcher{results: []model.ChannelResult{{Channel: "siren", Required: true, Delivered: true}}},
		&stubRetries{},
		store,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(delivery, store, nil)

	raised, err := service.Raise(context.Background(), "V-03", model.AlarmLowLevel, "level below threshold")
	if err != nil {
		t.Fatal(err)
	}
	if raised.State != model.AlarmDelivered {
		t.Fatalf("state=%s", raised.State)
	}

	restored := restart(t, root)
	replayed, ok := restored.Get(raised.ID)
	if !ok {
		t.Fatal("alarm missing after restart")
	}
	if replayed.State != model.AlarmDelivered {
		t.Fatalf("state after restart=%s", replayed.State)
	}
}

func TestRecoveryIgnoresSuppressionEvents(t *testing.T) {
	root := t.TempDir()
	store, err := journal.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(nil, store, nil)
	alarm := deliveredAlarm(t, time.Unix(1000, 0))
	service.Restore(alarm)
	if _, err := service.Acknowledge(alarm.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSuppression(journal.NewSuppressionState(alarm.ID.String(), time.Unix(1000, 0), time.Minute), time.Unix(1000, 0)); err != nil {
		t.Fatal(err)
	}

	restored := restart(t, root)
	replayed, ok := restored.Get(alarm.ID)
	if !ok {
		t.Fatal("alarm missing after restart")
	}
	if replayed.State != model.AlarmAcknowledged {
		t.Fatalf("state after restart=%s", replayed.State)
	}
}

func TestConcurrentTransitionsMatchJournalAndRestart(t *testing.T) {
	root := t.TempDir()
	store, err := journal.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(nil, store, nil)

	const alarmCount = 8
	ids := make([]uuid.UUID, 0, alarmCount)
	for range alarmCount {
		alarm := deliveredAlarm(t, time.Now())
		service.Restore(alarm)
		ids = append(ids, alarm.ID)
	}

	var wait sync.WaitGroup
	errs := make(chan error, alarmCount*8)
	for _, id := range ids {
		for range 4 {
			wait.Add(2)
			go func() {
				defer wait.Done()
				if _, err := service.Acknowledge(id); err != nil {
					errs <- err
				}
			}()
			go func() {
				defer wait.Done()
				if _, err := service.Clear(id); err != nil {
					errs <- err
				}
			}()
		}
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		// Acknowledge may legitimately lose the race against clear.
		if !errors.Is(err, ErrAlarmStateConflict) {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// Every alarm received at least one clear, and a cleared alarm can never
	// leave that state, so all of them must end cleared — in memory, in the
	// journal, and after a restart.
	restored := restart(t, root)
	for _, id := range ids {
		current, ok := service.Get(id)
		if !ok {
			t.Fatalf("alarm %s missing", id)
		}
		if current.State != model.AlarmCleared {
			t.Fatalf("alarm %s state=%s", id, current.State)
		}
		replayed, ok := restored.Get(id)
		if !ok {
			t.Fatalf("alarm %s missing after restart", id)
		}
		if replayed.State != current.State || !replayed.UpdatedAt.Equal(current.UpdatedAt) {
			t.Fatalf("alarm %s diverged across restart: %+v != %+v", id, replayed, current)
		}
	}
}
