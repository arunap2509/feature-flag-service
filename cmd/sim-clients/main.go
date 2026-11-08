package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"feature-flag-service/internal/constants"
	"feature-flag-service/internal/evaluator/engine"
	"feature-flag-service/internal/mgmt/model"
)

type MobileMetrics struct {
	activeConns   int64
	putsReceived  uint64
	patchReceived uint64
	reconnects    uint64
	errors        uint64
}

type ServerMetrics struct {
	evalCount uint64
	mu        sync.Mutex
	latencies []time.Duration
}

func (s *ServerMetrics) Record(dur time.Duration) {
	atomic.AddUint64(&s.evalCount, 1)
	s.mu.Lock()
	if len(s.latencies) < 50000 { // sample up to 50k for percentile calculation
		s.latencies = append(s.latencies, dur)
	}
	s.mu.Unlock()
}

func (s *ServerMetrics) Snapshot() (count uint64, p50, p95, p99, max time.Duration) {
	count = atomic.LoadUint64(&s.evalCount)
	s.mu.Lock()
	if len(s.latencies) == 0 {
		s.mu.Unlock()
		return
	}
	sorted := make([]time.Duration, len(s.latencies))
	copy(sorted, s.latencies)
	s.latencies = s.latencies[:0]
	s.mu.Unlock()

	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	n := len(sorted)
	p50 = sorted[n*50/100]
	p95 = sorted[n*95/100]
	p99 = sorted[n*99/100]
	max = sorted[n-1]
	return
}

func main() {
	gatewayFlag := flag.String("gateway", constants.DefaultGatewayURL, "Load Balancer Gateway URL for Mobile SSE")
	mgmtFlag := flag.String("mgmt", constants.DefaultMgmtAPIURL, "Management API URL for Server SDK sync")
	modeFlag := flag.String("mode", constants.ClientModeDual, "Simulation mode: dual, mobile, server")
	mobileCountFlag := flag.Int("mobile", 100, "Number of concurrent mobile SSE clients")
	serverWorkersFlag := flag.Int("server", 10, "Number of concurrent server SDK evaluation workers")
	envFlag := flag.String("env", "", "Environment ID (defaults to first active production environment)")
	durationFlag := flag.Duration("duration", 0, "Simulation duration (e.g. 1m, 5m, or 0 for indefinite)")
	flag.Parse()

	log.Printf("==================================================")
	log.Printf("📱 [sim-clients] Dual-Mode Client Fleet Simulator")
	log.Printf("   - Gateway URL:    %s", *gatewayFlag)
	log.Printf("   - Mgmt API URL:   %s", *mgmtFlag)
	log.Printf("   - Mode:           %s", *modeFlag)
	log.Printf("   - Mobile Clients: %d", *mobileCountFlag)
	log.Printf("   - Server Workers: %d", *serverWorkersFlag)
	if *durationFlag > 0 {
		log.Printf("   - Duration:       %v", *durationFlag)
	} else {
		log.Printf("   - Duration:       Indefinite (press Ctrl+C to stop)")
	}
	log.Printf("==================================================")

	client := &http.Client{Timeout: 5 * time.Second}

	// Resolve Target Environment
	envID := *envFlag
	if envID == "" {
		discovered, err := discoverFirstEnv(client, *mgmtFlag)
		if err != nil {
			log.Fatalf("[sim-clients] Could not discover environment from %s: %v", *mgmtFlag, err)
		}
		envID = discovered
	}
	log.Printf("[sim-clients] Operating on Environment ID: %s", envID)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *durationFlag > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *durationFlag)
		defer cancel()
	}

	mobileMetrics := &MobileMetrics{}
	serverMetrics := &ServerMetrics{}
	var wg sync.WaitGroup

	// ==========================================
	// 1. Mobile Fleet Simulator (SSE Streams)
	// ==========================================
	if *modeFlag == constants.ClientModeDual || *modeFlag == constants.ClientModeMobile {
		log.Printf("[sim-clients] Starting %d persistent Mobile SSE clients to %s...", *mobileCountFlag, *gatewayFlag)
		for i := 1; i <= *mobileCountFlag; i++ {
			wg.Add(1)
			userID := fmt.Sprintf("usr_mob_%04d", i)
			connID := fmt.Sprintf("conn_mob_%04d", i)
			go func(uID, cID string) {
				defer wg.Done()
				runMobileClient(ctx, *gatewayFlag, envID, uID, cID, mobileMetrics)
			}(userID, connID)
			// Smooth connection ramp-up (5ms between connects to prevent thundering herd)
			time.Sleep(5 * time.Millisecond)
		}
	}

	// ==========================================
	// 2. Server SDK Simulator (Local In-Memory AST)
	// ==========================================
	if *modeFlag == constants.ClientModeDual || *modeFlag == constants.ClientModeServer {
		log.Printf("[sim-clients] Starting Server SDK with %d worker routines...", *serverWorkersFlag)

		// Shared In-Memory Rule Store for Server SDK
		ruleStore := newServerRuleStore(client, *mgmtFlag, envID)
		if err := ruleStore.Sync(); err != nil {
			log.Printf("[sim-clients] Warning: Initial Server SDK rule sync failed: %v", err)
		}

		// Background periodic rule sync (every 5 seconds)
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if err := ruleStore.Sync(); err != nil {
						log.Printf("[sim-clients] [Server SDK] Sync error: %v", err)
					}
				}
			}
		}()

		// Launch in-memory evaluation worker routines
		for w := 1; w <= *serverWorkersFlag; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				runServerWorker(ctx, ruleStore, serverMetrics, workerID)
			}(w)
		}
	}

	// ==========================================
	// 3. Real-Time Telemetry Dashboard
	// ==========================================
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		var lastServerEvals uint64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if *modeFlag == constants.ClientModeDual || *modeFlag == constants.ClientModeMobile {
					active := atomic.LoadInt64(&mobileMetrics.activeConns)
					puts := atomic.LoadUint64(&mobileMetrics.putsReceived)
					patches := atomic.LoadUint64(&mobileMetrics.patchReceived)
					reconnects := atomic.LoadUint64(&mobileMetrics.reconnects)
					errs := atomic.LoadUint64(&mobileMetrics.errors)

					log.Printf("[sim-clients] [Mobile SSE] Connected: %4d/%d | Puts: %d, Patches: %d, Reconnects: %d, Err: %d",
						active, *mobileCountFlag, puts, patches, reconnects, errs)
				}

				if *modeFlag == constants.ClientModeDual || *modeFlag == constants.ClientModeServer {
					tot, p50, p95, p99, max := serverMetrics.Snapshot()
					instEvals := tot - lastServerEvals
					lastServerEvals = tot

					log.Printf("[sim-clients] [Server SDK] %7d evals/sec | Latency: p50=%v p95=%v p99=%v max=%v (< 1µs in-memory)",
						instEvals, p50.Round(time.Nanosecond), p95.Round(time.Nanosecond), p99.Round(time.Nanosecond), max.Round(time.Nanosecond))
				}
			}
		}
	}()

	<-ctx.Done()
	log.Println("[sim-clients] Stop signal received, shutting down clients...")
	wg.Wait()

	log.Printf("==================================================")
	log.Printf("📊 [sim-clients] Final Run Summary")
	if *modeFlag == constants.ClientModeDual || *modeFlag == constants.ClientModeMobile {
		log.Printf("   - Mobile SSE Puts Received:    %d", atomic.LoadUint64(&mobileMetrics.putsReceived))
		log.Printf("   - Mobile SSE Patches Received: %d", atomic.LoadUint64(&mobileMetrics.patchReceived))
		log.Printf("   - Mobile Stream Reconnects:    %d", atomic.LoadUint64(&mobileMetrics.reconnects))
	}
	if *modeFlag == constants.ClientModeDual || *modeFlag == constants.ClientModeServer {
		log.Printf("   - Server SDK Total In-Memory Evaluations: %d", atomic.LoadUint64(&serverMetrics.evalCount))
	}
	log.Printf("==================================================")
}

func discoverFirstEnv(client *http.Client, mgmtURL string) (string, error) {
	resp, err := client.Get(fmt.Sprintf("%s/api/v1/tenants", mgmtURL))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var tenants []model.Tenant
	if err := json.NewDecoder(resp.Body).Decode(&tenants); err != nil {
		return "", err
	}
	if len(tenants) == 0 {
		return "", fmt.Errorf("no tenants found")
	}

	for _, t := range tenants {
		envResp, err := client.Get(fmt.Sprintf("%s/api/v1/tenants/%s/environments", mgmtURL, t.ID))
		if err != nil {
			continue
		}
		var envs []model.Environment
		_ = json.NewDecoder(envResp.Body).Decode(&envs)
		envResp.Body.Close()

		for _, env := range envs {
			if strings.Contains(env.Name, "production") || strings.Contains(env.Name, "prod") {
				return env.ID, nil
			}
		}
		if len(envs) > 0 {
			return envs[0].ID, nil
		}
	}
	return "", fmt.Errorf("no environments found")
}

func runMobileClient(ctx context.Context, gatewayURL, envID, userID, connID string, metrics *MobileMetrics) {
	streamURL := fmt.Sprintf("%s/api/v1/stream?env_id=%s&user_id=%s&conn_id=%s", gatewayURL, envID, userID, connID)

	backoff := 100 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, streamURL, nil)
		if err != nil {
			atomic.AddUint64(&metrics.errors, 1)
			return
		}
		req.Header.Set("Accept", "text/event-stream")

		// Connect with no client timeout on the stream
		sseClient := &http.Client{Timeout: 0}
		resp, err := sseClient.Do(req)
		if err != nil {
			atomic.AddUint64(&metrics.reconnects, 1)
			time.Sleep(backoff)
			if backoff < 2*time.Second {
				backoff *= 2
			}
			continue
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			atomic.AddUint64(&metrics.reconnects, 1)
			time.Sleep(backoff)
			if backoff < 2*time.Second {
				backoff *= 2
			}
			continue
		}

		// Connected successfully! Reset backoff
		backoff = 100 * time.Millisecond
		atomic.AddInt64(&metrics.activeConns, 1)

		scanner := bufio.NewScanner(resp.Body)
		var currentEvent string
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "event:") {
				currentEvent = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			} else if strings.HasPrefix(line, "data:") {
				data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
				if currentEvent == constants.SSEEventPut {
					atomic.AddUint64(&metrics.putsReceived, 1)
				} else if currentEvent == constants.SSEEventPatch {
					atomic.AddUint64(&metrics.patchReceived, 1)
					_ = data // parsed in real-world SDK
				}
				currentEvent = ""
			}
		}

		resp.Body.Close()
		atomic.AddInt64(&metrics.activeConns, -1)
		atomic.AddUint64(&metrics.reconnects, 1)

		// Wait briefly before reconnecting
		time.Sleep(200 * time.Millisecond)
	}
}

type ServerRuleStore struct {
	client   *http.Client
	mgmtURL  string
	envID    string
	mu       sync.RWMutex
	flags    map[string]*engine.ParsedFlag
	segments map[string]*engine.ParsedSegment
}

func newServerRuleStore(client *http.Client, mgmtURL, envID string) *ServerRuleStore {
	return &ServerRuleStore{
		client:   client,
		mgmtURL:  mgmtURL,
		envID:    envID,
		flags:    make(map[string]*engine.ParsedFlag),
		segments: make(map[string]*engine.ParsedSegment),
	}
}

func (s *ServerRuleStore) Sync() error {
	// 1. Fetch flags
	flagURL := fmt.Sprintf("%s/api/v1/environments/%s/flags", s.mgmtURL, s.envID)
	flagResp, err := s.client.Get(flagURL)
	if err != nil {
		return err
	}
	defer flagResp.Body.Close()

	var flags []model.Flag
	if err := json.NewDecoder(flagResp.Body).Decode(&flags); err != nil {
		return err
	}

	// 2. Fetch segments
	segURL := fmt.Sprintf("%s/api/v1/environments/%s/segments", s.mgmtURL, s.envID)
	segResp, err := s.client.Get(segURL)
	var segments []model.Segment
	if err == nil {
		defer segResp.Body.Close()
		_ = json.NewDecoder(segResp.Body).Decode(&segments)
	}

	newFlags := make(map[string]*engine.ParsedFlag, len(flags))
	for i := range flags {
		newFlags[flags[i].Key] = engine.NewParsedFlag(&flags[i])
	}

	newSegments := make(map[string]*engine.ParsedSegment, len(segments))
	for i := range segments {
		newSegments[segments[i].Key] = engine.NewParsedSegment(&segments[i])
	}

	s.mu.Lock()
	s.flags = newFlags
	s.segments = newSegments
	s.mu.Unlock()
	return nil
}

func (s *ServerRuleStore) Snapshot() (map[string]*engine.ParsedFlag, map[string]*engine.ParsedSegment) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.flags, s.segments
}

func runServerWorker(ctx context.Context, store *ServerRuleStore, metrics *ServerMetrics, workerID int) {
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)*12345))
	users := []string{
		"usr_alice", "usr_bob", "usr_charlie", "usr_david",
		"usr_beta_005", "usr_beta_012", "usr_ent_001", "usr_ent_042",
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		flags, segments := store.Snapshot()
		if len(flags) == 0 {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		uID := users[r.Intn(len(users))]
		userCtx := engine.UserContext{UserID: uID}

		// Perform in-memory sub-microsecond evaluation of all flags
		evalStart := time.Now()
		_ = engine.EvaluateAll(flags, segments, userCtx)
		dur := time.Since(evalStart)

		metrics.Record(dur)
	}
}
