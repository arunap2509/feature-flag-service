package cache

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"feature-flag-service/internal/constants"
	"feature-flag-service/internal/evaluator/engine"
	"feature-flag-service/internal/mgmt/model"
)

// Store holds the active targeting rules and segments in memory per environment
type Store struct {
	mu          sync.RWMutex
	flags       map[string]map[string]*engine.ParsedFlag    // env_id -> flag_key -> ParsedFlag
	segments    map[string]map[string]*engine.ParsedSegment // env_id -> segment_key -> ParsedSegment
	envVersions map[string]int64                            // env_id -> latest_env_version
}

// NewStore initializes an empty in-memory store
func NewStore() *Store {
	return &Store{
		flags:       make(map[string]map[string]*engine.ParsedFlag),
		segments:    make(map[string]map[string]*engine.ParsedSegment),
		envVersions: make(map[string]int64),
	}
}

// ApplyEvent updates the in-memory rules based on an incoming CDC outbox event
func (s *Store) ApplyEvent(payload model.OutboxEventPayload) {
	s.mu.Lock()
	defer s.mu.Unlock()

	envID := payload.EnvironmentID
	if _, ok := s.flags[envID]; !ok {
		s.flags[envID] = make(map[string]*engine.ParsedFlag)
	}
	if _, ok := s.segments[envID]; !ok {
		s.segments[envID] = make(map[string]*engine.ParsedSegment)
	}

	// Update environment version
	if payload.EnvironmentVersion > s.envVersions[envID] {
		s.envVersions[envID] = payload.EnvironmentVersion
	}

	switch payload.EventType {
	case constants.EventTypeFlagCreated, constants.EventTypeFlagUpdated, constants.EventTypeFlagRolledBack:
		pf := &engine.ParsedFlag{
			Key:           payload.FlagKey,
			State:         payload.State,
			Version:       payload.FlagVersion,
			FallbackValue: payload.FallbackValue,
			Salt:          payload.Salt,
			Rules:         make([]engine.ParsedRule, 0),
		}
		if len(payload.Rules) > 0 {
			var rawRules []model.Rule
			if err := json.Unmarshal(payload.Rules, &rawRules); err == nil {
				for _, r := range rawRules {
					rollout := 0
					if r.RolloutPercentage != nil {
						rollout = *r.RolloutPercentage
					}
					pf.Rules = append(pf.Rules, engine.ParsedRule{
						Order:             r.Order,
						Segment:           r.Segment,
						Value:             r.Value,
						RolloutPercentage: rollout,
					})
				}
			}
		}
		s.flags[envID][payload.FlagKey] = pf
		log.Printf("[%s] Cached flag '%s' (v%d, state=%s, salt=%s) for env '%s'",
			constants.ServiceNameEvaluator, pf.Key, pf.Version, pf.State, pf.Salt, envID)

	case constants.EventTypeFlagKilled:
		existing, ok := s.flags[envID][payload.FlagKey]
		var salt string
		var rules []engine.ParsedRule
		if ok {
			salt = existing.Salt
			rules = existing.Rules
		} else {
			salt = payload.Salt
			rules = make([]engine.ParsedRule, 0)
		}
		// Allocate a brand-new immutable struct (Copy-on-Write) to eliminate concurrent read/write data races
		s.flags[envID][payload.FlagKey] = &engine.ParsedFlag{
			Key:           payload.FlagKey,
			State:         constants.FlagStateOff,
			Version:       payload.FlagVersion,
			FallbackValue: payload.FallbackValue,
			Salt:          salt,
			Rules:         rules,
		}
		log.Printf("[%s] Emergency Kill Switch applied: flag '%s' flipped OFF for env '%s'",
			constants.ServiceNameEvaluator, payload.FlagKey, envID)

	case constants.EventTypeSegmentCreated, constants.EventTypeSegmentUpdated:
		ps := &engine.ParsedSegment{
			Key:        payload.SegmentKey,
			UserIDsMap: make(map[string]struct{}),
		}
		if len(payload.UserIDs) > 0 {
			var userIDs []string
			if err := json.Unmarshal(payload.UserIDs, &userIDs); err == nil {
				for _, id := range userIDs {
					ps.UserIDsMap[id] = struct{}{}
				}
			}
		}
		s.segments[envID][payload.SegmentKey] = ps
		log.Printf("[%s] Cached segment '%s' (%d users) for env '%s'",
			constants.ServiceNameEvaluator, ps.Key, len(ps.UserIDsMap), envID)
	}
}

// GetEnvironmentData retrieves the parsed flags, segments, and current version for evaluation
func (s *Store) GetEnvironmentData(envID string) (map[string]*engine.ParsedFlag, map[string]*engine.ParsedSegment, int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	flagsCopy := make(map[string]*engine.ParsedFlag)
	if fMap, ok := s.flags[envID]; ok {
		for k, v := range fMap {
			flagsCopy[k] = v
		}
	}

	segmentsCopy := make(map[string]*engine.ParsedSegment)
	if sMap, ok := s.segments[envID]; ok {
		for k, v := range sMap {
			segmentsCopy[k] = v
		}
	}

	version := s.envVersions[envID]
	return flagsCopy, segmentsCopy, version
}

// HydrateFromDB loads all active flags and segments from PostgreSQL at evaluator startup
func (s *Store) HydrateFromDB(db *sql.DB) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Hydrate environments version
	envRows, err := db.Query(`SELECT id, environment_version FROM environments`)
	if err != nil {
		return fmt.Errorf("failed to query environments: %w", err)
	}
	defer envRows.Close()

	for envRows.Next() {
		var envID string
		var version int64
		if err := envRows.Scan(&envID, &version); err != nil {
			return err
		}
		s.envVersions[envID] = version
		if _, ok := s.flags[envID]; !ok {
			s.flags[envID] = make(map[string]*engine.ParsedFlag)
		}
		if _, ok := s.segments[envID]; !ok {
			s.segments[envID] = make(map[string]*engine.ParsedSegment)
		}
	}

	// 2. Hydrate segments
	segRows, err := db.Query(`SELECT environment_id, key, version, user_ids FROM segments`)
	if err == nil {
		defer segRows.Close()
		for segRows.Next() {
			var envID, key string
			var version int64
			var userIDsJSON []byte
			if err := segRows.Scan(&envID, &key, &version, &userIDsJSON); err == nil {
				ps := &engine.ParsedSegment{
					Key:        key,
					Version:    version,
					UserIDsMap: make(map[string]struct{}),
				}
				var userIDs []string
				if err := json.Unmarshal(userIDsJSON, &userIDs); err == nil {
					for _, u := range userIDs {
						ps.UserIDsMap[u] = struct{}{}
					}
				}
				if _, ok := s.segments[envID]; !ok {
					s.segments[envID] = make(map[string]*engine.ParsedSegment)
				}
				s.segments[envID][key] = ps
			}
		}
	}

	// 3. Hydrate flags
	flagRows, err := db.Query(`SELECT environment_id, key, state, version, rules, fallback_value, salt FROM flags`)
	if err != nil {
		return fmt.Errorf("failed to query flags: %w", err)
	}
	defer flagRows.Close()

	count := 0
	for flagRows.Next() {
		var envID, key, state, salt string
		var version int64
		var rulesJSON []byte
		var fallbackValue bool
		if err := flagRows.Scan(&envID, &key, &state, &version, &rulesJSON, &fallbackValue, &salt); err != nil {
			return err
		}

		pf := &engine.ParsedFlag{
			Key:           key,
			State:         state,
			Version:       version,
			FallbackValue: fallbackValue,
			Salt:          salt,
			Rules:         make([]engine.ParsedRule, 0),
		}
		if len(rulesJSON) > 0 {
			var rawRules []model.Rule
			if err := json.Unmarshal(rulesJSON, &rawRules); err == nil {
				for _, r := range rawRules {
					rollout := 0
					if r.RolloutPercentage != nil {
						rollout = *r.RolloutPercentage
					}
					pf.Rules = append(pf.Rules, engine.ParsedRule{
						Order:             r.Order,
						Segment:           r.Segment,
						Value:             r.Value,
						RolloutPercentage: rollout,
					})
				}
			}
		}

		if _, ok := s.flags[envID]; !ok {
			s.flags[envID] = make(map[string]*engine.ParsedFlag)
		}
		s.flags[envID][key] = pf
		count++
	}

	log.Printf("[%s] Hydrated %d flags across %d environments from PostgreSQL database",
		constants.ServiceNameEvaluator, count, len(s.flags))
	return nil
}
