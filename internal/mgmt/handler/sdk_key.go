package handler

import (
	"encoding/json"
	"net/http"

	"feature-flag-service/internal/constants"
	"feature-flag-service/internal/mgmt/model"
)

func (h *Handler) CreateSDKKey(w http.ResponseWriter, r *http.Request) {
	envID := r.PathValue("env_id")
	var req model.CreateSDKKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, constants.ErrMsgInvalidRequestBody)
		return
	}
	key, err := h.svc.CreateSDKKey(r.Context(), envID, req)
	if err != nil {
		h.handleError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, key)
}

func (h *Handler) ListSDKKeys(w http.ResponseWriter, r *http.Request) {
	envID := r.PathValue("env_id")
	keys, err := h.svc.ListSDKKeys(r.Context(), envID)
	if err != nil {
		h.handleError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, keys)
}
