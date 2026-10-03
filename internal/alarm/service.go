package alarm

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/journal"
	"github.com/wyw14/cry-98/internal/model"
)

// ErrAlarmNotFound is returned when no alarm matches the given id.
var ErrAlarmNotFound = errors.New("alarm not found")

// ErrInvalidTransition is returned when a command cannot be applied to the
// alarm's current lifecycle state (other than an idempotent retry).
var ErrInvalidTransition = errors.New("alarm state does not allow this transition")

const (
	deliveryEventKind    = "alarm.delivery"
	acknowledgeEventKind = "alarm.acknowledged"
	clearEventKind       = "alarm.cleared"
	alarmEventPartition  = "alarms"
)

type Service struct {
	mu       sync.Mutex
	alarms   map[uuid.UUID]model.Alarm
	delivery *DeliveryService
	journal  *journal.Store
	now      func() time.Time
}

func NewService(delivery *DeliveryService, store *journal.Store, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{
		alarms:   make(map[uuid.UUID]model.Alarm),
		delivery: delivery,
		journal:  store,
		now:      now,
	}
}

func (s *Service) Raise(ctx context.Context, vesselID string, kind model.AlarmKind, message string) (model.Alarm, error) {
	alarm, err := model.NewAlarm(vesselID, kind, message, s.now())
	if err != nil {
		return model.Alarm{}, err
	}
	s.mu.Lock()
	s.alarms[alarm.ID] = alarm
	s.mu.Unlock()
	delivered, err := s.delivery.Deliver(ctx, alarm)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		// Delivery persists its own journal record. When it reports an error
		// the record may be a partial delivery that retry will finish, so keep
		// the delivered (partial) view; a dispatch that never reached the
		// journal leaves the transient detected entry behind and must not be
		// observable as a delivered alarm.
		if delivered.ID != uuid.Nil {
			s.alarms[alarm.ID] = delivered
		} else {
			delete(s.alarms, alarm.ID)
		}
		return delivered, err
	}
	s.alarms[alarm.ID] = delivered
	return delivered, nil
}

func (s *Service) Restore(alarm model.Alarm) {
	s.mu.Lock()
	s.alarms[alarm.ID] = alarm
	s.mu.Unlock()
}

func (s *Service) Get(id uuid.UUID) (model.Alarm, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	alarm, ok := s.alarms[id]
	return alarm, ok
}

func (s *Service) List() []model.Alarm {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]model.Alarm, 0, len(s.alarms))
	for _, alarm := range s.alarms {
		result = append(result, alarm)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result
}

// Acknowledge marks an alarm as acknowledged. The journal record is written
// before the in-memory state is updated, so a successful response always
// survives a restart; a persistence failure leaves the previous state intact
// and returns an error. A duplicate request for an already acknowledged alarm
// is answered with the current state without appending another event.
func (s *Service) Acknowledge(id uuid.UUID) (model.Alarm, error) {
	return s.transition(id, model.AlarmAcknowledged, acknowledgeEventKind, func(a model.Alarm, now time.Time) model.Alarm {
		return a.Acknowledge(now)
	})
}

// Clear marks an alarm as cleared with the same durability and idempotency
// guarantees as Acknowledge.
func (s *Service) Clear(id uuid.UUID) (model.Alarm, error) {
	return s.transition(id, model.AlarmCleared, clearEventKind, func(a model.Alarm, now time.Time) model.Alarm {
		return a.Clear(now)
	})
}

func (s *Service) transition(
	id uuid.UUID,
	target model.AlarmState,
	eventKind string,
	apply func(model.Alarm, time.Time) model.Alarm,
) (model.Alarm, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	alarm, ok := s.alarms[id]
	if !ok {
		return model.Alarm{}, ErrAlarmNotFound
	}
	// Idempotent replay of the same command: report the persisted state with
	// no new journal event and no timestamp churn.
	if alarm.State == target {
		return alarm, nil
	}
	if !alarm.CanTransition(target) {
		return model.Alarm{}, ErrInvalidTransition
	}

	updated := apply(alarm, s.now())
	if s.journal != nil {
		event, err := model.NewEvent(alarmEventPartition, eventKind, id.String(), updated, s.now())
		if err != nil {
			return model.Alarm{}, err
		}
		// Write-ahead: only publish the new state after it is durable. On
		// failure the map keeps the previous state, so callers cannot observe
		// a success that a restart would undo.
		if _, err := s.journal.Append(event); err != nil {
			return model.Alarm{}, err
		}
	}
	s.alarms[id] = updated
	return updated, nil
}
