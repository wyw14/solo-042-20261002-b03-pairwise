package alarm

import (
	"encoding/json"

	"github.com/wyw14/cry-98/internal/journal"
	"github.com/wyw14/cry-98/internal/model"
)

type Recovery struct {
	store   *journal.Store
	service *Service
}

func NewRecovery(store *journal.Store, service *Service) *Recovery {
	return &Recovery{store: store, service: service}
}

func (r *Recovery) Restore() error {
	events, err := r.store.Events(eventPartition, 0)
	if err != nil {
		return err
	}
	// Events are replayed in journal order, so folding delivery and state
	// records together leaves each alarm at its last persisted state —
	// including acknowledgements and clears recorded after delivery.
	latest := make(map[string]model.Alarm)
	for _, event := range events {
		if event.Kind != eventKindDelivery && event.Kind != eventKindState {
			continue
		}
		var alarm model.Alarm
		if err := json.Unmarshal(event.Payload, &alarm); err != nil {
			return err
		}
		latest[alarm.ID.String()] = alarm
	}
	for _, alarm := range latest {
		r.service.Restore(alarm)
	}
	return nil
}
