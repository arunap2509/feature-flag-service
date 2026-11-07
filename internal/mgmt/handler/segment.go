package handler

import (
	"encoding/json"
	"fmt"
	"net/http"

	"feature-flag-service/internal/constants"
	"feature-flag-service/internal/mgmt/model"
)

func (h *Handler) CreateSegment(w http.ResponseWriter, r *http.Request) {
	envID := r.PathValue("env_id")
	var req model.CreateSegmentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, constants.ErrMsgInvalidRequestBody)
		return
	}
	changedBy := r.Header.Get(constants.HeaderUser)
	seg, err := h.svc.CreateSegment(r.Context(), envID, req, changedBy)
	if err != nil {
		h.handleError(w, err)
		return
	}
	w.Header().Set(constants.HeaderETag, fmt.Sprintf("\"%d\"", seg.Version))
	writeJSON(w, http.StatusCreated, seg)
}

func (h *Handler) GetSegment(w http.ResponseWriter, r *http.Request) {
	envID := r.PathValue("env_id")
	key := r.PathValue("segment_key")
	seg, err := h.svc.GetSegment(r.Context(), envID, key)
	if err != nil {
		h.handleError(w, err)
		return
	}
	w.Header().Set(constants.HeaderETag, fmt.Sprintf("\"%d\"", seg.Version))
	writeJSON(w, http.StatusOK, seg)
}

func (h *Handler) ListSegments(w http.ResponseWriter, r *http.Request) {
	envID := r.PathValue("env_id")
	segs, err := h.svc.ListSegments(r.Context(), envID)
	if err != nil {
		h.handleError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, segs)
}

func (h *Handler) UpdateSegment(w http.ResponseWriter, r *http.Request) {
	envID := r.PathValue("env_id")
	key := r.PathValue("segment_key")
	var req model.UpdateSegmentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, constants.ErrMsgInvalidRequestBody)
		return
	}
	ifMatch := r.Header.Get(constants.HeaderIfMatch)
	changedBy := r.Header.Get(constants.HeaderUser)
	seg, err := h.svc.UpdateSegment(r.Context(), envID, key, req, ifMatch, changedBy)
	if err != nil {
		h.handleError(w, err)
		return
	}
	w.Header().Set(constants.HeaderETag, fmt.Sprintf("\"%d\"", seg.Version))
	writeJSON(w, http.StatusOK, seg)
}
