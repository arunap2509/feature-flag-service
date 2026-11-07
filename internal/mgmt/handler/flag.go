package handler

import (
	"encoding/json"
	"net/http"
	"strconv"

	"feature-flag-service/internal/constants"
	"feature-flag-service/internal/mgmt/model"
)

func (h *Handler) CreateFlag(w http.ResponseWriter, r *http.Request) {
	caller, err := extractCaller(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	envID := r.PathValue("env_id")
	var req model.CreateFlagRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, constants.ErrMsgInvalidRequestBody)
		return
	}

	flag, err := h.svc.CreateFlag(r.Context(), envID, req, caller)
	if err != nil {
		h.handleError(w, err)
		return
	}

	w.Header().Set(constants.HeaderETag, strconv.FormatInt(flag.Version, 10))
	writeJSON(w, http.StatusCreated, flag)
}

func (h *Handler) GetFlag(w http.ResponseWriter, r *http.Request) {
	envID := r.PathValue("env_id")
	flagKey := r.PathValue("flag_key")
	flag, err := h.svc.GetFlag(r.Context(), envID, flagKey)
	if err != nil {
		h.handleError(w, err)
		return
	}
	w.Header().Set(constants.HeaderETag, strconv.FormatInt(flag.Version, 10))
	writeJSON(w, http.StatusOK, flag)
}

func (h *Handler) ListFlags(w http.ResponseWriter, r *http.Request) {
	envID := r.PathValue("env_id")
	flags, err := h.svc.ListFlags(r.Context(), envID)
	if err != nil {
		h.handleError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, flags)
}

func (h *Handler) UpdateFlag(w http.ResponseWriter, r *http.Request) {
	caller, err := extractCaller(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	envID := r.PathValue("env_id")
	flagKey := r.PathValue("flag_key")
	ifMatch := r.Header.Get(constants.HeaderIfMatch)

	var req model.UpdateFlagRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, constants.ErrMsgInvalidRequestBody)
		return
	}

	flag, err := h.svc.UpdateFlag(r.Context(), envID, flagKey, req, ifMatch, caller)
	if err != nil {
		h.handleError(w, err)
		return
	}

	w.Header().Set(constants.HeaderETag, strconv.FormatInt(flag.Version, 10))
	writeJSON(w, http.StatusOK, flag)
}

func (h *Handler) KillFlag(w http.ResponseWriter, r *http.Request) {
	caller, err := extractCaller(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	envID := r.PathValue("env_id")
	flagKey := r.PathValue("flag_key")

	flag, err := h.svc.KillFlag(r.Context(), envID, flagKey, caller)
	if err != nil {
		h.handleError(w, err)
		return
	}

	w.Header().Set(constants.HeaderETag, strconv.FormatInt(flag.Version, 10))
	writeJSON(w, http.StatusOK, flag)
}

func (h *Handler) RollbackFlag(w http.ResponseWriter, r *http.Request) {
	caller, err := extractCaller(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	envID := r.PathValue("env_id")
	flagKey := r.PathValue("flag_key")
	targetVersionStr := r.PathValue("target_version")
	targetVersion, err := strconv.ParseInt(targetVersionStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, constants.ErrMsgInvalidTargetVersion)
		return
	}

	flag, err := h.svc.RollbackFlag(r.Context(), envID, flagKey, targetVersion, caller)
	if err != nil {
		h.handleError(w, err)
		return
	}

	w.Header().Set(constants.HeaderETag, strconv.FormatInt(flag.Version, 10))
	writeJSON(w, http.StatusOK, flag)
}

func (h *Handler) GetFlagHistory(w http.ResponseWriter, r *http.Request) {
	envID := r.PathValue("env_id")
	flagKey := r.PathValue("flag_key")
	history, err := h.svc.GetFlagHistory(r.Context(), envID, flagKey)
	if err != nil {
		h.handleError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, history)
}
