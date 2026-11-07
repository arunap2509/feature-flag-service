package model

import (
	"encoding/json"
	"time"
)

// ==========================================
// 1. Domain Entities (Database Schema Models)
// ==========================================

type Tenant struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

type Environment struct {
	ID                 string    `json:"id"`
	TenantID           string    `json:"tenant_id"`
	Name               string    `json:"name"`
	EnvironmentVersion int64     `json:"environment_version"`
	ShardCount         int       `json:"shard_count"`
	CreatedAt          time.Time `json:"created_at"`
}

type SDKKey struct {
	ID            int64     `json:"id"`
	EnvironmentID string    `json:"environment_id"`
	Key           string    `json:"key"`
	KeyType       string    `json:"key_type"` // 'client' or 'server'
	CreatedAt     time.Time `json:"created_at"`
}

type Rule struct {
	Order             int    `json:"order"`
	Segment           string `json:"segment,omitempty"`
	RolloutPercentage *int   `json:"rollout_percentage,omitempty"` // 0 to 100
	Value             bool   `json:"value"`
}

type Flag struct {
	ID            int64           `json:"id"`
	EnvironmentID string          `json:"environment_id"`
	Key           string          `json:"key"`
	State         string          `json:"state"` // 'on' or 'off'
	Version       int64           `json:"version"`
	Rules         json.RawMessage `json:"rules"`
	FallbackValue bool            `json:"fallback_value"`
	Salt          string          `json:"salt"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

type Segment struct {
	ID            int64           `json:"id"`
	EnvironmentID string          `json:"environment_id"`
	Key           string          `json:"key"`
	Version       int64           `json:"version"`
	UserIDs       json.RawMessage `json:"user_ids"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

type FlagHistory struct {
	ID            int64           `json:"id"`
	FlagID        int64           `json:"flag_id"`
	Version       int64           `json:"version"`
	State         string          `json:"state"`
	Rules         json.RawMessage `json:"rules"`
	FallbackValue bool            `json:"fallback_value"`
	Salt          string          `json:"salt"`
	ChangedBy     string          `json:"changed_by"`
	CreatedAt     time.Time       `json:"created_at"`
}

type OutboxRecord struct {
	ID          int64           `json:"id"`
	EnvID       string          `json:"env_id"`
	EnvVersion  int64           `json:"env_version"`
	EventType   string          `json:"event_type"`
	Payload     json.RawMessage `json:"payload"`
	CreatedAt   time.Time       `json:"created_at"`
}

type OutboxEventPayload struct {
	EventType          string          `json:"event_type"`
	EnvironmentID      string          `json:"environment_id"`
	EnvironmentVersion int64           `json:"environment_version"`
	FlagKey            string          `json:"flag_key,omitempty"`
	SegmentKey         string          `json:"segment_key,omitempty"`
	State              string          `json:"state,omitempty"`
	FlagVersion        int64           `json:"flag_version,omitempty"`
	Rules              json.RawMessage `json:"rules,omitempty"`
	FallbackValue      bool            `json:"fallback_value,omitempty"`
	Salt               string          `json:"salt,omitempty"`
	UserIDs            json.RawMessage `json:"user_ids,omitempty"`
	ChangedBy          string          `json:"changed_by"`
	Timestamp          time.Time       `json:"timestamp"`
}

// ==========================================
// 2. Request DTOs (Client Input Contracts)
// ==========================================

type CreateTenantRequest struct {
	Name string `json:"name"`
}

type CreateEnvironmentRequest struct {
	Name string `json:"name"`
}

type CreateSDKKeyRequest struct {
	Key     string `json:"key,omitempty"` // Optional, auto-generated if blank
	KeyType string `json:"key_type"`      // 'client' or 'server'
}

type CreateFlagRequest struct {
	Key           string          `json:"key"`
	State         string          `json:"state,omitempty"` // defaults to 'off'
	Rules         json.RawMessage `json:"rules,omitempty"` // defaults to '[]'
	FallbackValue bool            `json:"fallback_value"`
}

type UpdateFlagRequest struct {
	State         *string         `json:"state,omitempty"`
	Rules         json.RawMessage `json:"rules,omitempty"`
	FallbackValue *bool           `json:"fallback_value,omitempty"`
	ReshuffleSalt bool            `json:"reshuffle_salt,omitempty"`
}

type CreateSegmentRequest struct {
	Key     string   `json:"key"`
	UserIDs []string `json:"user_ids,omitempty"`
}

type UpdateSegmentRequest struct {
	UserIDs []string `json:"user_ids"`
}

// ==========================================
// 3. Response DTOs (Public Output Contracts)
// ==========================================

type TenantResponse struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

type EnvironmentResponse struct {
	ID                 string    `json:"id"`
	TenantID           string    `json:"tenant_id"`
	Name               string    `json:"name"`
	EnvironmentVersion int64     `json:"environment_version"`
	ShardCount         int       `json:"shard_count"`
	CreatedAt          time.Time `json:"created_at"`
}

type SDKKeyResponse struct {
	ID            int64     `json:"id"`
	EnvironmentID string    `json:"environment_id"`
	Key           string    `json:"key"`
	KeyType       string    `json:"key_type"`
	CreatedAt     time.Time `json:"created_at"`
}

type FlagResponse struct {
	ID            int64           `json:"id"`
	EnvironmentID string          `json:"environment_id"`
	Key           string          `json:"key"`
	State         string          `json:"state"`
	Version       int64           `json:"version"`
	Rules         json.RawMessage `json:"rules"`
	FallbackValue bool            `json:"fallback_value"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

type SegmentResponse struct {
	ID            int64           `json:"id"`
	EnvironmentID string          `json:"environment_id"`
	Key           string          `json:"key"`
	Version       int64           `json:"version"`
	UserIDs       json.RawMessage `json:"user_ids"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

type FlagHistoryResponse struct {
	ID            int64           `json:"id"`
	FlagID        int64           `json:"flag_id"`
	Version       int64           `json:"version"`
	State         string          `json:"state"`
	Rules         json.RawMessage `json:"rules"`
	FallbackValue bool            `json:"fallback_value"`
	ChangedBy     string          `json:"changed_by"`
	CreatedAt     time.Time       `json:"created_at"`
}

// ==========================================
// 4. Mappers (Entity -> Response DTO)
// ==========================================

func ToTenantResponse(t *Tenant) *TenantResponse {
	if t == nil {
		return nil
	}
	return &TenantResponse{
		ID:        t.ID,
		Name:      t.Name,
		CreatedAt: t.CreatedAt,
	}
}

func ToTenantResponses(tenants []Tenant) []TenantResponse {
	out := make([]TenantResponse, len(tenants))
	for i, t := range tenants {
		out[i] = *ToTenantResponse(&t)
	}
	return out
}

func ToEnvironmentResponse(e *Environment) *EnvironmentResponse {
	if e == nil {
		return nil
	}
	return &EnvironmentResponse{
		ID:                 e.ID,
		TenantID:           e.TenantID,
		Name:               e.Name,
		EnvironmentVersion: e.EnvironmentVersion,
		ShardCount:         e.ShardCount,
		CreatedAt:          e.CreatedAt,
	}
}

func ToEnvironmentResponses(envs []Environment) []EnvironmentResponse {
	out := make([]EnvironmentResponse, len(envs))
	for i, e := range envs {
		out[i] = *ToEnvironmentResponse(&e)
	}
	return out
}

func ToSDKKeyResponse(k *SDKKey) *SDKKeyResponse {
	if k == nil {
		return nil
	}
	return &SDKKeyResponse{
		ID:            k.ID,
		EnvironmentID: k.EnvironmentID,
		Key:           k.Key,
		KeyType:       k.KeyType,
		CreatedAt:     k.CreatedAt,
	}
}

func ToSDKKeyResponses(keys []SDKKey) []SDKKeyResponse {
	out := make([]SDKKeyResponse, len(keys))
	for i, k := range keys {
		out[i] = *ToSDKKeyResponse(&k)
	}
	return out
}

func ToFlagResponse(f *Flag) *FlagResponse {
	if f == nil {
		return nil
	}
	return &FlagResponse{
		ID:            f.ID,
		EnvironmentID: f.EnvironmentID,
		Key:           f.Key,
		State:         f.State,
		Version:       f.Version,
		Rules:         f.Rules,
		FallbackValue: f.FallbackValue,
		UpdatedAt:     f.UpdatedAt,
	}
}

func ToFlagResponses(flags []Flag) []FlagResponse {
	out := make([]FlagResponse, len(flags))
	for i, f := range flags {
		out[i] = *ToFlagResponse(&f)
	}
	return out
}

func ToSegmentResponse(s *Segment) *SegmentResponse {
	if s == nil {
		return nil
	}
	return &SegmentResponse{
		ID:            s.ID,
		EnvironmentID: s.EnvironmentID,
		Key:           s.Key,
		Version:       s.Version,
		UserIDs:       s.UserIDs,
		UpdatedAt:     s.UpdatedAt,
	}
}

func ToSegmentResponses(segs []Segment) []SegmentResponse {
	out := make([]SegmentResponse, len(segs))
	for i, s := range segs {
		out[i] = *ToSegmentResponse(&s)
	}
	return out
}

func ToFlagHistoryResponse(h *FlagHistory) *FlagHistoryResponse {
	if h == nil {
		return nil
	}
	return &FlagHistoryResponse{
		ID:            h.ID,
		FlagID:        h.FlagID,
		Version:       h.Version,
		State:         h.State,
		Rules:         h.Rules,
		FallbackValue: h.FallbackValue,
		ChangedBy:     h.ChangedBy,
		CreatedAt:     h.CreatedAt,
	}
}

func ToFlagHistoryResponses(history []FlagHistory) []FlagHistoryResponse {
	out := make([]FlagHistoryResponse, len(history))
	for i, h := range history {
		out[i] = *ToFlagHistoryResponse(&h)
	}
	return out
}
