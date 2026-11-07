package handler

import (
	"net/http"

	"feature-flag-service/internal/constants"
)

func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  constants.StatusHealthy,
		"service": constants.ServiceNameMgmtAPI,
	})
}
