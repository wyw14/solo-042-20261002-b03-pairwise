package api

import (
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/alarm"
	"github.com/wyw14/cry-98/internal/model"
)

type raiseAlarmRequest struct {
	VesselID string          `json:"vessel_id"`
	Kind     model.AlarmKind `json:"kind"`
	Message  string          `json:"message"`
}

type suppressAlarmRequest struct {
	Seconds int `json:"seconds"`
}

func (s *Server) listAlarms(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"items": s.deps.Alarms.List()})
}

func (s *Server) raiseAlarm(writer http.ResponseWriter, request *http.Request) {
	var input raiseAlarmRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	alarmValue, err := s.deps.Alarms.Raise(request.Context(), input.VesselID, input.Kind, input.Message)
	if err != nil && alarmValue.ID == uuid.Nil {
		if errors.Is(err, alarm.ErrAlarmPersistence) {
			writeError(writer, http.StatusInternalServerError, err.Error())
			return
		}
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(writer, http.StatusCreated, alarmValue)
}

func (s *Server) acknowledgeAlarm(writer http.ResponseWriter, request *http.Request) {
	id, err := uuid.Parse(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid alarm id")
		return
	}
	alarmValue, err := s.deps.Alarms.Acknowledge(id)
	if err != nil {
		writeAlarmStateError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, alarmValue)
}

func (s *Server) clearAlarm(writer http.ResponseWriter, request *http.Request) {
	id, err := uuid.Parse(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid alarm id")
		return
	}
	alarmValue, err := s.deps.Alarms.Clear(id)
	if err != nil {
		writeAlarmStateError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, alarmValue)
}

// writeAlarmStateError maps transition failures to status codes. A state
// change that could not be journaled is a server error — never a success.
func writeAlarmStateError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, alarm.ErrAlarmNotFound):
		writeError(writer, http.StatusNotFound, err.Error())
	case errors.Is(err, alarm.ErrAlarmStateConflict):
		writeError(writer, http.StatusConflict, err.Error())
	default:
		writeError(writer, http.StatusInternalServerError, err.Error())
	}
}

func (s *Server) suppressAlarm(writer http.ResponseWriter, request *http.Request) {
	id, err := uuid.Parse(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid alarm id")
		return
	}
	var input suppressAlarmRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.deps.Suppressions.Suppress(id, time.Duration(input.Seconds)*time.Second); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"alarm_id": id, "remaining_seconds": input.Seconds})
}
