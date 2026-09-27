package internal

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

type API struct {
	publisher EventPublisher
}

func NewAPI(publisher EventPublisher) *API {
	return &API{
		publisher: publisher,
	}
}

func (a *API) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/health", a.handleHealth)
	mux.HandleFunc("/events", a.handleEvents)
}

func (a *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok\n"))
}

func (a *API) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event Event

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&event); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if strings.TrimSpace(event.ID) == "" ||
		strings.TrimSpace(event.Type) == "" ||
		strings.TrimSpace(event.Source) == "" ||
		strings.TrimSpace(event.Key) == "" ||
		event.Timestamp.IsZero() ||
		event.Timestamp.After(time.Now()) {
		http.Error(w, "invalid event fields", http.StatusBadRequest)
		return
	}

	if err := a.publisher.Publish(r.Context(), event); err != nil {
		http.Error(w, "failed to publish event", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("event accepted\n"))
}
