package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"

	"feature-flag-service/internal/constants"
	"feature-flag-service/internal/mgmt/model"
)

type TenantSeed struct {
	ID   string
	Name string
}

type FlagSeed struct {
	Key           string
	State         string
	Rules         []model.Rule
	FallbackValue bool
}

func main() {
	dbURLFlag := flag.String("db", "", "PostgreSQL connection string (defaults to DATABASE_URL or localhost:5440)")
	cleanFlag := flag.Bool("clean", false, "Clean existing fleet data before seeding")
	flag.Parse()

	dbURL := *dbURLFlag
	if dbURL == "" {
		dbURL = os.Getenv("DATABASE_URL")
	}
	if dbURL == "" {
		dbURL = constants.DefaultLocalPostgresURL
	}

	log.Printf("[seeder] Connecting to PostgreSQL at %s...", dbURL)
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("[seeder] Failed to connect: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("[seeder] Database ping failed: %v", err)
	}

	ctx := context.Background()

	if *cleanFlag {
		log.Println("[seeder] Cleaning previous test data...")
		if _, err := db.ExecContext(ctx, "TRUNCATE outbox, flag_history, flags, segments, sdk_keys, environments, tenants CASCADE;"); err != nil {
			log.Printf("[seeder] Warning during truncate: %v", err)
		}
	}

	// 1. Define 5 Tenants
	tenants := []TenantSeed{
		{ID: "ten_acme_corp_global", Name: "Acme Corp"},
		{ID: "ten_globex_financial", Name: "Globex Financial"},
		{ID: "ten_initech_cloud_ops", Name: "Initech Cloud"},
		{ID: "ten_soylent_health_ai", Name: "Soylent Health"},
		{ID: "ten_umbrella_retail_us", Name: "Umbrella Retail"},
	}

	// 2. Define 20 Flags Per Environment
	intPtr := func(i int) *int { return &i }
	flagTemplates := []FlagSeed{
		{
			Key:   "checkout_v2",
			State: constants.FlagStateOn,
			Rules: []model.Rule{
				{Order: 1, Segment: constants.SegmentEnterpriseTier, Value: true},
				{Order: 2, RolloutPercentage: intPtr(25), Value: true},
			},
			FallbackValue: false,
		},
		{
			Key:   "ai_copilot_assistant",
			State: constants.FlagStateOn,
			Rules: []model.Rule{
				{Order: 1, Segment: constants.SegmentBetaTesters, Value: true},
				{Order: 2, RolloutPercentage: intPtr(15), Value: true},
			},
			FallbackValue: false,
		},
		{
			Key:   "dark_mode_v3",
			State: constants.FlagStateOn,
			Rules: []model.Rule{
				{Order: 1, RolloutPercentage: intPtr(100), Value: true},
			},
			FallbackValue: false,
		},
		{
			Key:           "quantum_search_indexer",
			State:         constants.FlagStateOff,
			Rules:         []model.Rule{},
			FallbackValue: false,
		},
		{
			Key:   "fraud_detection_ml",
			State: constants.FlagStateOn,
			Rules: []model.Rule{
				{Order: 1, RolloutPercentage: intPtr(50), Value: true},
			},
			FallbackValue: false,
		},
		{
			Key:   "one_click_order",
			State: constants.FlagStateOn,
			Rules: []model.Rule{
				{Order: 1, Segment: constants.SegmentEnterpriseTier, Value: true},
			},
			FallbackValue: false,
		},
		{
			Key:   "realtime_telemetry",
			State: constants.FlagStateOn,
			Rules: []model.Rule{
				{Order: 1, RolloutPercentage: intPtr(10), Value: true},
			},
			FallbackValue: false,
		},
		{
			Key:           "new_billing_portal",
			State:         constants.FlagStateOff,
			Rules:         []model.Rule{},
			FallbackValue: false,
		},
		{
			Key:   "dynamic_pricing_engine",
			State: constants.FlagStateOn,
			Rules: []model.Rule{
				{Order: 1, RolloutPercentage: intPtr(75), Value: true},
			},
			FallbackValue: false,
		},
		{
			Key:   "stream_video_4k",
			State: constants.FlagStateOn,
			Rules: []model.Rule{
				{Order: 1, Segment: constants.SegmentEnterpriseTier, Value: true},
			},
			FallbackValue: false,
		},
		{
			Key:   "smart_notifications",
			State: constants.FlagStateOn,
			Rules: []model.Rule{
				{Order: 1, RolloutPercentage: intPtr(30), Value: true},
			},
			FallbackValue: false,
		},
		{
			Key:           "redesign_homepage_hero",
			State:         constants.FlagStateOff,
			Rules:         []model.Rule{},
			FallbackValue: false,
		},
		{
			Key:           "instant_refunds",
			State:         constants.FlagStateOff,
			Rules:         []model.Rule{},
			FallbackValue: false,
		},
		{
			Key:   "biometric_auth_mfa",
			State: constants.FlagStateOn,
			Rules: []model.Rule{
				{Order: 1, RolloutPercentage: intPtr(60), Value: true},
			},
			FallbackValue: false,
		},
		{
			Key:   "latency_optimizer_cache",
			State: constants.FlagStateOn,
			Rules: []model.Rule{
				{Order: 1, RolloutPercentage: intPtr(100), Value: true},
			},
			FallbackValue: false,
		},
		{
			Key:   "referral_reward_v2",
			State: constants.FlagStateOn,
			Rules: []model.Rule{
				{Order: 1, RolloutPercentage: intPtr(20), Value: true},
			},
			FallbackValue: false,
		},
		{
			Key:           "experimental_voice_ui",
			State:         constants.FlagStateOff,
			Rules:         []model.Rule{},
			FallbackValue: false,
		},
		{
			Key:   "high_frequency_sync",
			State: constants.FlagStateOn,
			Rules: []model.Rule{
				{Order: 1, Segment: constants.SegmentBetaTesters, Value: true},
			},
			FallbackValue: false,
		},
		{
			Key:   "advanced_analytics_dashboard",
			State: constants.FlagStateOn,
			Rules: []model.Rule{
				{Order: 1, RolloutPercentage: intPtr(50), Value: true},
			},
			FallbackValue: false,
		},
		{
			Key:   "zero_trust_network_policy",
			State: constants.FlagStateOn,
			Rules: []model.Rule{
				{Order: 1, RolloutPercentage: intPtr(100), Value: true},
			},
			FallbackValue: false,
		},
	}

	// 3. Define Segment User IDs
	betaUsers := make([]string, 50)
	for i := 0; i < 50; i++ {
		betaUsers[i] = fmt.Sprintf("usr_beta_%03d", i+1)
	}
	betaUsersJSON, _ := json.Marshal(betaUsers)

	entUsers := make([]string, 50)
	for i := 0; i < 50; i++ {
		entUsers[i] = fmt.Sprintf("usr_ent_%03d", i+1)
	}
	entUsersJSON, _ := json.Marshal(entUsers)

	totalTenants := 0
	totalEnvironments := 0
	totalSegments := 0
	totalFlags := 0

	for _, t := range tenants {
		// Insert tenant
		_, err := db.ExecContext(ctx, `
			INSERT INTO tenants (id, name) VALUES ($1, $2)
			ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
		`, t.ID, t.Name)
		if err != nil {
			log.Fatalf("[seeder] Failed to insert tenant %s: %v", t.Name, err)
		}
		totalTenants++

		envNames := []string{"production", "staging"}
		for _, envName := range envNames {
			envID := fmt.Sprintf("env_%s_%s", t.ID[4:], envName)
			// Truncate to 64 chars if needed
			if len(envID) > 64 {
				envID = envID[:64]
			}

			// Upsert environment with shard_count = 3
			var currentEnvVersion int64 = 1
			err := db.QueryRowContext(ctx, `
				INSERT INTO environments (id, tenant_id, name, shard_count, environment_version)
				VALUES ($1, $2, $3, 3, 1)
				ON CONFLICT (tenant_id, name) DO UPDATE SET shard_count = 3
				RETURNING id, environment_version
			`, envID, t.ID, envName).Scan(&envID, &currentEnvVersion)
			if err != nil {
				log.Fatalf("[seeder] Failed to upsert environment %s for tenant %s: %v", envName, t.Name, err)
			}
			totalEnvironments++

			// Ensure Client & Server SDK Keys
			clientKey := fmt.Sprintf("%s%s_%s", constants.PrefixSDKClient, envName, randomHex(8))
			serverKey := fmt.Sprintf("%s%s_%s", constants.PrefixSDKServer, envName, randomHex(8))

			_, _ = db.ExecContext(ctx, `
				INSERT INTO sdk_keys (environment_id, key, key_type)
				VALUES ($1, $2, $3)
				ON CONFLICT (key) DO NOTHING
			`, envID, clientKey, constants.KeyTypeClient)

			_, _ = db.ExecContext(ctx, `
				INSERT INTO sdk_keys (environment_id, key, key_type)
				VALUES ($1, $2, $3)
				ON CONFLICT (key) DO NOTHING
			`, envID, serverKey, constants.KeyTypeServer)

			// Upsert Segments
			segmentsToSeed := []struct {
				Key     string
				UserIDs []byte
			}{
				{Key: constants.SegmentBetaTesters, UserIDs: betaUsersJSON},
				{Key: constants.SegmentEnterpriseTier, UserIDs: entUsersJSON},
			}

			for _, seg := range segmentsToSeed {
				var segID int64
				err := db.QueryRowContext(ctx, `
					INSERT INTO segments (environment_id, key, version, user_ids)
					VALUES ($1, $2, 1, $3)
					ON CONFLICT (environment_id, key) DO UPDATE SET user_ids = EXCLUDED.user_ids
					RETURNING id
				`, envID, seg.Key, seg.UserIDs).Scan(&segID)
				if err != nil {
					log.Fatalf("[seeder] Failed to upsert segment %s in %s: %v", seg.Key, envID, err)
				}
				totalSegments++

				// Increment environment version & write outbox event
				currentEnvVersion++
				_, _ = db.ExecContext(ctx, `UPDATE environments SET environment_version = $1 WHERE id = $2`, currentEnvVersion, envID)

				outboxPayload := model.OutboxEventPayload{
					EventType:          constants.EventTypeSegmentCreated,
					EnvironmentID:      envID,
					EnvironmentVersion: currentEnvVersion,
					SegmentKey:         seg.Key,
					UserIDs:            seg.UserIDs,
					ChangedBy:          "system_seeder",
					Timestamp:          time.Now().UTC(),
				}
				payloadBytes, _ := json.Marshal(outboxPayload)
				_, _ = db.ExecContext(ctx, `
					INSERT INTO outbox (env_id, env_version, event_type, payload)
					VALUES ($1, $2, $3, $4)
				`, envID, currentEnvVersion, constants.EventTypeSegmentCreated, payloadBytes)
			}

			// Upsert 20 Flags
			for _, flagTpl := range flagTemplates {
				rulesBytes, _ := json.Marshal(flagTpl.Rules)
				salt := randomHex(constants.DefaultSaltByteLength)

				var flagID int64
				err := db.QueryRowContext(ctx, `
					INSERT INTO flags (environment_id, key, state, version, rules, fallback_value, salt)
					VALUES ($1, $2, $3, 1, $4, $5, $6)
					ON CONFLICT (environment_id, key) DO UPDATE SET
						state = EXCLUDED.state,
						rules = EXCLUDED.rules,
						fallback_value = EXCLUDED.fallback_value
					RETURNING id
				`, envID, flagTpl.Key, flagTpl.State, rulesBytes, flagTpl.FallbackValue, salt).Scan(&flagID)
				if err != nil {
					log.Fatalf("[seeder] Failed to upsert flag %s in %s: %v", flagTpl.Key, envID, err)
				}
				totalFlags++

				// Increment environment version
				currentEnvVersion++
				_, _ = db.ExecContext(ctx, `UPDATE environments SET environment_version = $1 WHERE id = $2`, currentEnvVersion, envID)

				// Insert flag_history
				_, _ = db.ExecContext(ctx, `
					INSERT INTO flag_history (flag_id, version, state, rules, fallback_value, salt, changed_by)
					VALUES ($1, 1, $2, $3, $4, $5, $6)
				`, flagID, flagTpl.State, rulesBytes, flagTpl.FallbackValue, salt, "system_seeder")

				// Insert outbox event
				outboxPayload := model.OutboxEventPayload{
					EventType:          constants.EventTypeFlagCreated,
					EnvironmentID:      envID,
					EnvironmentVersion: currentEnvVersion,
					FlagKey:            flagTpl.Key,
					State:              flagTpl.State,
					FlagVersion:        1,
					Rules:              rulesBytes,
					FallbackValue:      flagTpl.FallbackValue,
					Salt:               salt,
					ChangedBy:          "system_seeder",
					Timestamp:          time.Now().UTC(),
				}
				payloadBytes, _ := json.Marshal(outboxPayload)
				_, _ = db.ExecContext(ctx, `
					INSERT INTO outbox (env_id, env_version, event_type, payload)
					VALUES ($1, $2, $3, $4)
				`, envID, currentEnvVersion, constants.EventTypeFlagCreated, payloadBytes)
			}
		}
	}

	log.Printf("==================================================")
	log.Printf("🌱 [seeder] Database Seeding Complete!")
	log.Printf("   - Tenants Seeded:      %d", totalTenants)
	log.Printf("   - Environments Seeded: %d (All with shard_count=3)", totalEnvironments)
	log.Printf("   - Segments Seeded:     %d (50 users each)", totalSegments)
	log.Printf("   - Flags Seeded:        %d (with full outbox & history)", totalFlags)
	log.Printf("==================================================")
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
