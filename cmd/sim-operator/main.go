package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"feature-flag-service/internal/constants"
	"feature-flag-service/internal/mgmt/model"
)

type FlagItem struct {
	EnvID         string
	Key           string
	State         string
	Version       int64
	Rules         []model.Rule
	FallbackValue bool
}

type Metrics struct {
	mu           sync.Mutex
	latencies    []time.Duration
	successCount uint64
	conflict412  uint64
	errorCount   uint64
	totalOps     uint64
}

func (m *Metrics) Record(dur time.Duration, status int) {
	atomic.AddUint64(&m.totalOps, 1)
	if status == http.StatusOK || status == http.StatusCreated {
		atomic.AddUint64(&m.successCount, 1)
	} else if status == http.StatusPreconditionFailed {
		atomic.AddUint64(&m.conflict412, 1)
	} else {
		atomic.AddUint64(&m.errorCount, 1)
	}

	m.mu.Lock()
	m.latencies = append(m.latencies, dur)
	m.mu.Unlock()
}

func (m *Metrics) Snapshot() (total, success, conflicts, errors uint64, p50, p95, p99, max time.Duration) {
	total = atomic.LoadUint64(&m.totalOps)
	success = atomic.LoadUint64(&m.successCount)
	conflicts = atomic.LoadUint64(&m.conflict412)
	errors = atomic.LoadUint64(&m.errorCount)

	m.mu.Lock()
	if len(m.latencies) == 0 {
		m.mu.Unlock()
		return
	}
	sorted := make([]time.Duration, len(m.latencies))
	copy(sorted, m.latencies)
	m.latencies = m.latencies[:0] // reset for interval rate calculation
	m.mu.Unlock()

	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	n := len(sorted)
	p50 = sorted[n*50/100]
	p95 = sorted[n*95/100]
	p99 = sorted[n*99/100]
	max = sorted[n-1]
	return
}

func main() {
	targetFlag := flag.String("target", constants.DefaultMgmtAPIURL, "Management API target URL")
	rateFlag := flag.Int("rate", constants.DefaultOperatorRate, "Target writes per second (10 - 100)")
	concurrencyFlag := flag.Int("concurrency", 4, "Number of concurrent operator worker routines")
	scenarioFlag := flag.String("scenario", constants.ScenarioMixed, "Scenario: mixed, rollout, toggle, kill")
	durationFlag := flag.Duration("duration", 0, "Execution duration (e.g. 30s, 2m, or 0 for indefinite)")
	envFilterFlag := flag.String("env", "", "Filter to specific environment ID (optional)")
	flag.Parse()

	targetURL := *targetFlag
	targetRate := *rateFlag
	if targetRate <= 0 {
		targetRate = constants.DefaultOperatorRate
	}
	concurrency := *concurrencyFlag
	if concurrency <= 0 {
		concurrency = 4
	}

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	log.Printf("==================================================")
	log.Printf("🚀 [sim-operator] Feature Flag Operator Simulator")
	log.Printf("   - Target API:     %s", targetURL)
	log.Printf("   - Target Rate:    %d writes/sec", targetRate)
	log.Printf("   - Concurrency:    %d workers", concurrency)
	log.Printf("   - Scenario:       %s", *scenarioFlag)
	if *durationFlag > 0 {
		log.Printf("   - Duration:       %v", *durationFlag)
	} else {
		log.Printf("   - Duration:       Indefinite (press Ctrl+C to stop)")
	}
	log.Printf("==================================================")

	// Phase 1: Fleet Discovery
	log.Println("[sim-operator] Discovering tenants, environments, and flags...")
	flagPool, err := discoverFleet(client, targetURL, *envFilterFlag)
	if err != nil {
		log.Fatalf("[sim-operator] Fleet discovery failed: %v", err)
	}
	if len(flagPool) == 0 {
		log.Fatalf("[sim-operator] No flags discovered in target environments! Please seed data first.")
	}
	log.Printf("[sim-operator] Discovered %d total flags ready for simulated operations.", len(flagPool))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *durationFlag > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *durationFlag)
		defer cancel()
	}

	var poolMu sync.RWMutex
	metrics := &Metrics{}

	// Rate limiter token channel
	tokens := make(chan struct{}, targetRate)
	go func() {
		ticker := time.NewTicker(time.Second / time.Duration(targetRate))
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				select {
				case tokens <- struct{}{}:
				default:
				}
			}
		}
	}()

	// Launch worker routines
	var wg sync.WaitGroup
	for workerID := 1; workerID <= concurrency; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*1000))
			for {
				select {
				case <-ctx.Done():
					return
				case <-tokens:
					// Pick a random flag from the pool
					poolMu.RLock()
					item := flagPool[r.Intn(len(flagPool))]
					poolMu.RUnlock()

					dur, status, updatedVersion := executeOperatorAction(ctx, client, targetURL, item, *scenarioFlag, id, r)
					metrics.Record(dur, status)

					// If successful, update version in pool
					if (status == http.StatusOK || status == http.StatusCreated) && updatedVersion > 0 {
						poolMu.Lock()
						for i := range flagPool {
							if flagPool[i].EnvID == item.EnvID && flagPool[i].Key == item.Key {
								flagPool[i].Version = updatedVersion
								break
							}
						}
						poolMu.Unlock()
					} else if status == http.StatusPreconditionFailed {
						// On 412, refresh flag from API so subsequent writes have the latest version
						freshVersion, freshState, freshRules := refreshFlag(ctx, client, targetURL, item.EnvID, item.Key)
						if freshVersion > 0 {
							poolMu.Lock()
							for i := range flagPool {
								if flagPool[i].EnvID == item.EnvID && flagPool[i].Key == item.Key {
									flagPool[i].Version = freshVersion
									flagPool[i].State = freshState
									flagPool[i].Rules = freshRules
									break
								}
							}
							poolMu.Unlock()
						}
					}
				}
			}
		}(workerID)
	}

	// Metrics reporter loop
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		var lastTotal, lastSuccess, lastConflicts, lastErrors uint64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				tot, succ, conf, errs, p50, p95, p99, max := metrics.Snapshot()
				instTotal := tot - lastTotal
				instSucc := succ - lastSuccess
				instConf := conf - lastConflicts
				instErr := errs - lastErrors
				lastTotal, lastSuccess, lastConflicts, lastErrors = tot, succ, conf, errs

				log.Printf("[sim-operator] %3d writes/s (ok: %d, 412 OCC: %d, err: %d) | Latency: p50=%v p95=%v p99=%v max=%v",
					instTotal, instSucc, instConf, instErr, p50.Round(time.Microsecond), p95.Round(time.Microsecond), p99.Round(time.Microsecond), max.Round(time.Microsecond))
			}
		}
	}()

	<-ctx.Done()
	log.Println("[sim-operator] Shutdown signal received, waiting for workers to terminate...")
	wg.Wait()

	// Final Summary Report
	tot := atomic.LoadUint64(&metrics.totalOps)
	succ := atomic.LoadUint64(&metrics.successCount)
	conf := atomic.LoadUint64(&metrics.conflict412)
	errs := atomic.LoadUint64(&metrics.errorCount)
	log.Printf("==================================================")
	log.Printf("📊 [sim-operator] Final Run Summary")
	log.Printf("   - Total Operations Attempted: %d", tot)
	log.Printf("   - Successful Writes (200 OK):  %d (%.2f%%)", succ, float64(succ)/float64(tot+1)*100)
	log.Printf("   - OCC 412 Conflicts Detected: %d (%.2f%%)", conf, float64(conf)/float64(tot+1)*100)
	log.Printf("   - System Errors:              %d", errs)
	log.Printf("==================================================")
}

func discoverFleet(client *http.Client, baseURL string, envFilter string) ([]FlagItem, error) {
	var pool []FlagItem

	// 1. Fetch tenants
	resp, err := client.Get(fmt.Sprintf("%s/api/v1/tenants", baseURL))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch tenants: %w", err)
	}
	defer resp.Body.Close()

	var tenants []model.Tenant
	if err := json.NewDecoder(resp.Body).Decode(&tenants); err != nil {
		return nil, fmt.Errorf("failed to decode tenants: %w", err)
	}

	for _, t := range tenants {
		// Fetch environments for tenant
		envResp, err := client.Get(fmt.Sprintf("%s/api/v1/tenants/%s/environments", baseURL, t.ID))
		if err != nil {
			continue
		}
		var envs []model.Environment
		_ = json.NewDecoder(envResp.Body).Decode(&envs)
		envResp.Body.Close()

		for _, env := range envs {
			if envFilter != "" && env.ID != envFilter {
				continue
			}

			// Fetch flags for environment
			flagResp, err := client.Get(fmt.Sprintf("%s/api/v1/environments/%s/flags", baseURL, env.ID))
			if err != nil {
				continue
			}
			var flags []model.Flag
			_ = json.NewDecoder(flagResp.Body).Decode(&flags)
			flagResp.Body.Close()

			for _, f := range flags {
				var rules []model.Rule
				if len(f.Rules) > 0 {
					_ = json.Unmarshal(f.Rules, &rules)
				}
				pool = append(pool, FlagItem{
					EnvID:         env.ID,
					Key:           f.Key,
					State:         f.State,
					Version:       f.Version,
					Rules:         rules,
					FallbackValue: f.FallbackValue,
				})
			}
		}
	}
	return pool, nil
}

func executeOperatorAction(ctx context.Context, client *http.Client, baseURL string, item FlagItem, scenario string, workerID int, r *rand.Rand) (time.Duration, int, int64) {
	action := scenario
	if action == constants.ScenarioMixed {
		roll := r.Intn(100)
		if roll < 60 {
			action = constants.ScenarioRollout
		} else if roll < 90 {
			action = constants.ScenarioToggle
		} else {
			action = constants.ScenarioKill
		}
	}

	start := time.Now()
	caller := fmt.Sprintf("operator_worker_%02d", workerID)
	strPtr := func(s string) *string { return &s }

	switch action {
	case constants.ScenarioKill:
		url := fmt.Sprintf("%s/api/v1/environments/%s/flags/%s/kill", baseURL, item.EnvID, item.Key)
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
		req.Header.Set(constants.HeaderUser, caller)
		resp, err := client.Do(req)
		dur := time.Since(start)
		if err != nil {
			return dur, http.StatusInternalServerError, 0
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		etag := resp.Header.Get(constants.HeaderETag)
		v, _ := strconv.ParseInt(etag, 10, 64)
		return dur, resp.StatusCode, v

	case constants.ScenarioToggle:
		newState := constants.FlagStateOn
		if item.State == constants.FlagStateOn {
			newState = constants.FlagStateOff
		}
		rulesJSON, _ := json.Marshal(item.Rules)
		reqBody := model.UpdateFlagRequest{
			State:         strPtr(newState),
			Rules:         rulesJSON,
			FallbackValue: &item.FallbackValue,
		}
		return sendUpdateRequest(ctx, client, baseURL, item.EnvID, item.Key, item.Version, reqBody, caller, start)

	case constants.ScenarioRollout:
		// Modify rollout percentage
		rolloutPcts := []int{10, 25, 40, 50, 75, 90, 100}
		pct := rolloutPcts[r.Intn(len(rolloutPcts))]

		rules := []model.Rule{
			{Order: 1, RolloutPercentage: &pct, Value: true},
		}
		rulesJSON, _ := json.Marshal(rules)
		reqBody := model.UpdateFlagRequest{
			State:         strPtr(constants.FlagStateOn),
			Rules:         rulesJSON,
			FallbackValue: &item.FallbackValue,
		}
		return sendUpdateRequest(ctx, client, baseURL, item.EnvID, item.Key, item.Version, reqBody, caller, start)

	default:
		return sendUpdateRequest(ctx, client, baseURL, item.EnvID, item.Key, item.Version, model.UpdateFlagRequest{
			State:         strPtr(item.State),
			FallbackValue: &item.FallbackValue,
		}, caller, start)
	}
}

func sendUpdateRequest(ctx context.Context, client *http.Client, baseURL, envID, flagKey string, version int64, reqBody model.UpdateFlagRequest, caller string, start time.Time) (time.Duration, int, int64) {
	url := fmt.Sprintf("%s/api/v1/environments/%s/flags/%s", baseURL, envID, flagKey)
	bodyBytes, _ := json.Marshal(reqBody)

	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(bodyBytes))
	req.Header.Set(constants.HeaderContentType, constants.ContentTypeJSON)
	req.Header.Set(constants.HeaderUser, caller)
	req.Header.Set(constants.HeaderIfMatch, strconv.FormatInt(version, 10))

	resp, err := client.Do(req)
	dur := time.Since(start)
	if err != nil {
		return dur, http.StatusInternalServerError, 0
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	etag := resp.Header.Get(constants.HeaderETag)
	v, _ := strconv.ParseInt(etag, 10, 64)
	return dur, resp.StatusCode, v
}

func refreshFlag(ctx context.Context, client *http.Client, baseURL, envID, flagKey string) (int64, string, []model.Rule) {
	url := fmt.Sprintf("%s/api/v1/environments/%s/flags/%s", baseURL, envID, flagKey)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			resp.Body.Close()
		}
		return 0, "", nil
	}
	defer resp.Body.Close()

	var f model.Flag
	if err := json.NewDecoder(resp.Body).Decode(&f); err != nil {
		return 0, "", nil
	}
	var rules []model.Rule
	if len(f.Rules) > 0 {
		_ = json.Unmarshal(f.Rules, &rules)
	}
	return f.Version, f.State, rules
}
