package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"feature-flag-service/internal/constants"
	"feature-flag-service/internal/mgmt/repository"
	"feature-flag-service/internal/mgmt/service"
)

type Handler struct {
	svc *service.Service
}

func New(svc *service.Service) *Handler {
	return &Handler{svc: svc}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	// Health
	mux.HandleFunc("GET /healthz", h.HealthCheck)

	// Tenants
	mux.HandleFunc("POST /api/v1/tenants", h.CreateTenant)
	mux.HandleFunc("GET /api/v1/tenants", h.ListTenants)
	mux.HandleFunc("GET /api/v1/tenants/{tenant_id}", h.GetTenant)

	// Environments
	mux.HandleFunc("POST /api/v1/tenants/{tenant_id}/environments", h.CreateEnvironment)
	mux.HandleFunc("GET /api/v1/tenants/{tenant_id}/environments", h.ListEnvironments)
	mux.HandleFunc("GET /api/v1/environments/{env_id}", h.GetEnvironment)

	// SDK Keys
	mux.HandleFunc("POST /api/v1/environments/{env_id}/keys", h.CreateSDKKey)
	mux.HandleFunc("GET /api/v1/environments/{env_id}/keys", h.ListSDKKeys)

	// Flags
	mux.HandleFunc("POST /api/v1/environments/{env_id}/flags", h.CreateFlag)
	mux.HandleFunc("GET /api/v1/environments/{env_id}/flags", h.ListFlags)
	mux.HandleFunc("GET /api/v1/environments/{env_id}/flags/{flag_key}", h.GetFlag)
	mux.HandleFunc("PUT /api/v1/environments/{env_id}/flags/{flag_key}", h.UpdateFlag)
	mux.HandleFunc("POST /api/v1/environments/{env_id}/flags/{flag_key}/kill", h.KillFlag)
	mux.HandleFunc("POST /api/v1/environments/{env_id}/flags/{flag_key}/rollback/{target_version}", h.RollbackFlag)
	mux.HandleFunc("GET /api/v1/environments/{env_id}/flags/{flag_key}/history", h.GetFlagHistory)

	// Segments
	mux.HandleFunc("POST /api/v1/environments/{env_id}/segments", h.CreateSegment)
	mux.HandleFunc("GET /api/v1/environments/{env_id}/segments", h.ListSegments)
	mux.HandleFunc("GET /api/v1/environments/{env_id}/segments/{segment_key}", h.GetSegment)
	mux.HandleFunc("PUT /api/v1/environments/{env_id}/segments/{segment_key}", h.UpdateSegment)

	// Outbox & Real-time Stream Monitoring
	mux.HandleFunc("GET /api/v1/outbox", h.GetRecentOutbox)
}

func (h *Handler) GetRecentOutbox(w http.ResponseWriter, r *http.Request) {
	limit := constants.DefaultOutboxLimit
	records, err := h.svc.GetRecentOutbox(r.Context(), limit)
	if err != nil {
		h.handleError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, records)
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func (h *Handler) handleError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, repository.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, repository.ErrVersionMismatch):
		writeError(w, http.StatusPreconditionFailed, err.Error())
	default:
		writeError(w, http.StatusBadRequest, err.Error())
	}
}

func extractCaller(r *http.Request) (string, error) {
	caller := strings.TrimSpace(r.Header.Get(constants.HeaderUser))
	if caller == "" {
		return "", errors.New(constants.ErrMsgCallerIdentityRequired)
	}
	return caller, nil
}
