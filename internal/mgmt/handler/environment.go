package handler

import (
	"encoding/json"
	"net/http"

	"feature-flag-service/internal/constants"
	"feature-flag-service/internal/mgmt/model"
)

func (h *Handler) CreateEnvironment(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	var req model.CreateEnvironmentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, constants.ErrMsgInvalidRequestBody)
		return
	}
	env, err := h.svc.CreateEnvironment(r.Context(), tenantID, req)
	if err != nil {
		h.handleError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, env)
}

func (h *Handler) GetEnvironment(w http.ResponseWriter, r *http.Request) {
	envID := r.PathValue("env_id")
	env, err := h.svc.GetEnvironment(r.Context(), envID)
	if err != nil {
		h.handleError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, env)
}

func (h *Handler) ListEnvironments(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	envs, err := h.svc.ListEnvironments(r.Context(), tenantID)
	if err != nil {
		h.handleError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, envs)
}
