package alarm

import (
	"encoding/json"

	"github.com/wyw14/cry-98/internal/journal"
	"github.com/wyw14/cry-98/internal/model"
)

// Event kinds that carry a full alarm snapshot as their payload. Replay must
// apply every one of them in journal order so that acknowledge/clear changes
// survive restarts instead of reverting to the last delivery record.
var alarmSnapshotEventKinds = map[string]bool{
	"alarm.delivery":     true,
	"alarm.acknowledged": true,
	"alarm.cleared":      true,
}

type Recovery struct {
	store   *journal.Store
	service *Service
}

func NewRecovery(store *journal.Store, service *Service) *Recovery {
	return &Recovery{store: store, service: service}
}

func (r *Recovery) Restore() error {
	events, err := r.store.Events("alarms", 0)
	if err != nil {
		return err
	}
	// Events arrive in sequence order; fold each alarm snapshot over the
	// stream so the final map holds the latest durable state per alarm.
	latest := make(map[string]model.Alarm)
	for _, event := range events {
		if !alarmSnapshotEventKinds[event.Kind] {
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
