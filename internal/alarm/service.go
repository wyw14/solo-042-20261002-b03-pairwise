package alarm

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
)

var (
	// ErrAlarmNotFound is returned when the alarm id is unknown to the service.
	ErrAlarmNotFound = errors.New("alarm not found")
	// ErrAlarmStateConflict is returned when the requested transition is not
	// allowed from the alarm's current state (e.g. acknowledging a cleared alarm).
	ErrAlarmStateConflict = errors.New("alarm state conflict")
	// ErrAlarmPersistence is returned when the state change could not be
	// durably recorded; the in-memory state is left unchanged in that case.
	ErrAlarmPersistence = errors.New("alarm state could not be persisted")
)

type Service struct {
	mu       sync.RWMutex
	alarms   map[uuid.UUID]model.Alarm
	delivery *DeliveryService
	journal  EventAppender
	now      func() time.Time
}

func NewService(delivery *DeliveryService, journal EventAppender, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{alarms: make(map[uuid.UUID]model.Alarm), delivery: delivery, journal: journal, now: now}
}

func (s *Service) Raise(ctx context.Context, vesselID string, kind model.AlarmKind, message string) (model.Alarm, error) {
	alarm, err := model.NewAlarm(vesselID, kind, message, s.now())
	if err != nil {
		return model.Alarm{}, err
	}
	// The alarm becomes visible only after its delivery outcome has been
	// persisted; a delivery that could not be journaled leaves no trace.
	delivered, err := s.delivery.Deliver(ctx, alarm)
	if delivered.ID == uuid.Nil {
		return model.Alarm{}, fmt.Errorf("%w: %v", ErrAlarmPersistence, err)
	}
	s.mu.Lock()
	s.alarms[delivered.ID] = delivered
	s.mu.Unlock()
	return delivered, err
}

func (s *Service) Restore(alarm model.Alarm) {
	s.mu.Lock()
	s.alarms[alarm.ID] = alarm
	s.mu.Unlock()
}

func (s *Service) Get(id uuid.UUID) (model.Alarm, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	alarm, ok := s.alarms[id]
	return alarm, ok
}

func (s *Service) List() []model.Alarm {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]model.Alarm, 0, len(s.alarms))
	for _, alarm := range s.alarms {
		result = append(result, alarm)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result
}

func (s *Service) Acknowledge(id uuid.UUID) (model.Alarm, error) {
	return s.transition(id, model.AlarmAcknowledged)
}

func (s *Service) Clear(id uuid.UUID) (model.Alarm, error) {
	return s.transition(id, model.AlarmCleared)
}

// transition moves an alarm to the target state using write-ahead semantics:
// the new state is journaled first and the in-memory map is updated only
// after the journal append succeeds, so a restart always replays exactly
// what was reported to the caller. Repeating a transition that is already in
// the target state is an idempotent no-op and is not journaled again.
func (s *Service) transition(id uuid.UUID, target model.AlarmState) (model.Alarm, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	alarm, ok := s.alarms[id]
	if !ok {
		return model.Alarm{}, ErrAlarmNotFound
	}
	if alarm.State == target {
		return alarm, nil
	}
	if alarm.State == model.AlarmCleared {
		return model.Alarm{}, fmt.Errorf("%w: alarm %s is already cleared", ErrAlarmStateConflict, id)
	}
	switch target {
	case model.AlarmAcknowledged:
		alarm = alarm.Acknowledge(s.now())
	case model.AlarmCleared:
		alarm = alarm.Clear(s.now())
	default:
		return model.Alarm{}, fmt.Errorf("%w: unsupported target state %q", ErrAlarmStateConflict, target)
	}
	event, err := model.NewEvent(eventPartition, eventKindState, alarm.ID.String(), alarm, s.now())
	if err != nil {
		return model.Alarm{}, fmt.Errorf("%w: %v", ErrAlarmPersistence, err)
	}
	if _, err := s.journal.Append(event); err != nil {
		return model.Alarm{}, fmt.Errorf("%w: %v", ErrAlarmPersistence, err)
	}
	s.alarms[id] = alarm
	return alarm, nil
}
