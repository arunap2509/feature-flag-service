package engine

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"

	"feature-flag-service/internal/constants"
	"feature-flag-service/internal/mgmt/model"
)

// UserContext represents the client attributes passed during evaluation
type UserContext struct {
	UserID     string            `json:"user_id"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// ParsedRule is a compiled in-memory representation of a flag targeting rule
type ParsedRule struct {
	Order             int    `json:"order"`
	Segment           string `json:"segment,omitempty"`
	Value             bool   `json:"value"`
	RolloutPercentage int    `json:"rollout_percentage,omitempty"`
}

// ParsedFlag is an optimized in-memory flag representation for sub-microsecond checks
type ParsedFlag struct {
	Key           string       `json:"key"`
	State         string       `json:"state"`
	Version       int64        `json:"version"`
	FallbackValue bool         `json:"fallback_value"`
	Salt          string       `json:"salt"`
	Rules         []ParsedRule `json:"rules"`
}

// ParsedSegment is an optimized in-memory segment representation
type ParsedSegment struct {
	Key        string              `json:"key"`
	Version    int64               `json:"version"`
	UserIDsMap map[string]struct{} `json:"-"`
}

// NewParsedFlag converts a model.Flag into an optimized in-memory flag
func NewParsedFlag(f *model.Flag) *ParsedFlag {
	pf := &ParsedFlag{
		Key:           f.Key,
		State:         f.State,
		Version:       f.Version,
		FallbackValue: f.FallbackValue,
		Salt:          f.Salt,
		Rules:         make([]ParsedRule, 0),
	}

	if len(f.Rules) > 0 {
		var rawRules []model.Rule
		if err := json.Unmarshal(f.Rules, &rawRules); err == nil {
			for _, r := range rawRules {
				rollout := 0
				if r.RolloutPercentage != nil {
					rollout = *r.RolloutPercentage
				}
				pf.Rules = append(pf.Rules, ParsedRule{
					Order:             r.Order,
					Segment:           r.Segment,
					Value:             r.Value,
					RolloutPercentage: rollout,
				})
			}
		}
	}
	return pf
}

// NewParsedSegment converts a model.Segment into an optimized in-memory segment
func NewParsedSegment(s *model.Segment) *ParsedSegment {
	ps := &ParsedSegment{
		Key:        s.Key,
		Version:    s.Version,
		UserIDsMap: make(map[string]struct{}),
	}
	if len(s.UserIDs) > 0 {
		var userIDs []string
		if err := json.Unmarshal(s.UserIDs, &userIDs); err == nil {
			for _, id := range userIDs {
				ps.UserIDsMap[id] = struct{}{}
			}
		}
	}
	return ps
}

// ComputeBucket calculates deterministic hash bucket: hash(key + salt + user_id) % 100,000
func ComputeBucket(flagKey, salt, userID string) uint64 {
	h := sha256.New()
	h.Write([]byte(flagKey))
	h.Write([]byte(salt))
	h.Write([]byte(userID))
	sum := h.Sum(nil)
	val := binary.BigEndian.Uint64(sum[:8])
	return val % constants.RolloutBucketModulus
}

// EvaluateFlag evaluates a single flag against a user context
func EvaluateFlag(flag *ParsedFlag, segments map[string]*ParsedSegment, ctx UserContext) bool {
	// 1. Emergency Kill Switch fast path: if state is 'off', return fallback value immediately
	if flag.State != constants.FlagStateOn {
		return flag.FallbackValue
	}

	// 2. Evaluate targeting rules in strict order
	for _, rule := range flag.Rules {
		// A. Segment check (if specified)
		if rule.Segment != "" {
			seg, exists := segments[rule.Segment]
			if !exists {
				continue // Segment not defined, skip rule
			}
			if _, inSet := seg.UserIDsMap[ctx.UserID]; !inSet {
				continue // User not in segment, skip rule
			}
		}

		// B. Percentage Rollout check (if specified)
		if rule.RolloutPercentage > 0 {
			bucket := ComputeBucket(flag.Key, flag.Salt, ctx.UserID)
			threshold := uint64(rule.RolloutPercentage * constants.RolloutMultiplier)
			if bucket >= threshold {
				continue // Outside percentage bucket, skip rule
			}
		}

		// Rule matched!
		return rule.Value
	}

	// 3. Fallback value if no rules matched
	return flag.FallbackValue
}

// EvaluateAll evaluates all active flags in an environment for a user context
func EvaluateAll(flags map[string]*ParsedFlag, segments map[string]*ParsedSegment, ctx UserContext) map[string]bool {
	results := make(map[string]bool, len(flags))
	for key, flag := range flags {
		results[key] = EvaluateFlag(flag, segments, ctx)
	}
	return results
}

// ComputeShard calculates the shard ID for a given user ID
func ComputeShard(userID string, shardCount int) int {
	if shardCount <= 1 {
		return 0
	}
	h := sha256.Sum256([]byte(userID))
	val := binary.BigEndian.Uint64(h[:8])
	return int(val % uint64(shardCount))
}
