package id

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"feature-flag-service/internal/constants"
)

// New generates a time-sortable, cryptographically random, prefixed identifier.
// Format: <prefix><12-char hex timestamp><16-char hex random bytes>
// Example: ten_019318b7a4214f8a9e012345
func New(prefix string) string {
	timestampMs := time.Now().UTC().UnixMilli()
	randomBytes := make([]byte, constants.DefaultIDRandomBytes)
	if _, err := rand.Read(randomBytes); err != nil {
		// Fallback to timestamp only if crypto/rand fails
		return fmt.Sprintf("%s%012x", prefix, timestampMs)
	}
	return fmt.Sprintf("%s%012x%s", prefix, timestampMs, hex.EncodeToString(randomBytes))
}
