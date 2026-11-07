package handler

import (
	"encoding/json"
	"net/http"

	"feature-flag-service/internal/constants"
	"feature-flag-service/internal/mgmt/model"
)

func (h *Handler) CreateTenant(w http.ResponseWriter, r *http.Request) {
	var req model.CreateTenantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, constants.ErrMsgInvalidRequestBody)
		return
	}
	tenant, err := h.svc.CreateTenant(r.Context(), req)
	if err != nil {
		h.handleError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, tenant)
}

func (h *Handler) GetTenant(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("tenant_id")
	tenant, err := h.svc.GetTenant(r.Context(), id)
	if err != nil {
		h.handleError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, tenant)
}

func (h *Handler) ListTenants(w http.ResponseWriter, r *http.Request) {
	tenants, err := h.svc.ListTenants(r.Context())
	if err != nil {
		h.handleError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, tenants)
}
