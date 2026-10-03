package alarm

import (
	"context"
	"errors"
	"time"

	"github.com/wyw14/cry-98/internal/model"
)

const (
	eventPartition    = "alarms"
	eventKindDelivery = "alarm.delivery"
	eventKindState    = "alarm.state"
)

type Dispatcher interface {
	Dispatch(context.Context, model.Alarm) ([]model.ChannelResult, error)
}

type RetryScheduler interface {
	Schedule(model.Alarm)
}

// EventAppender is the write side of the alarm journal. Implementations must
// durably persist the event before returning.
type EventAppender interface {
	Append(model.Event) (model.Event, error)
}

type DeliveryService struct {
	dispatcher Dispatcher
	retries    RetryScheduler
	journal    EventAppender
	now        func() time.Time
}

func NewDeliveryService(dispatcher Dispatcher, retries RetryScheduler, store EventAppender, now func() time.Time) (*DeliveryService, error) {
	if dispatcher == nil || retries == nil || store == nil {
		return nil, errors.New("alarm delivery dependencies are required")
	}
	if now == nil {
		now = time.Now
	}
	return &DeliveryService{dispatcher: dispatcher, retries: retries, journal: store, now: now}, nil
}

func (s *DeliveryService) Deliver(ctx context.Context, alarm model.Alarm) (model.Alarm, error) {
	alarm.State = model.AlarmDispatching
	results, dispatchErr := s.dispatcher.Dispatch(ctx, alarm)
	alarm = alarm.WithChannels(results, s.now())
	event, err := model.NewEvent(eventPartition, eventKindDelivery, alarm.ID.String(), alarm, s.now())
	if err != nil {
		return model.Alarm{}, err
	}
	if _, err := s.journal.Append(event); err != nil {
		return model.Alarm{}, err
	}
	if dispatchErr != nil {
		s.retries.Schedule(alarm)
		return alarm, dispatchErr
	}
	return alarm, nil
}
