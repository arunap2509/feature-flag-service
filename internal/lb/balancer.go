package lb

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"feature-flag-service/internal/constants"
)

// UpstreamPool dynamically manages healthy Connection Server backends discovered via etcd
type UpstreamPool struct {
	client   *clientv3.Client
	targets  []*url.URL
	counter  uint64
	mu       sync.RWMutex
	stopChan chan struct{}
}

// NewUpstreamPool creates a pool connected to etcd for dynamic service discovery
func NewUpstreamPool(endpoints []string) (*UpstreamPool, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd: %w", err)
	}

	return &UpstreamPool{
		client:   cli,
		targets:  make([]*url.URL, 0),
		stopChan: make(chan struct{}),
	}, nil
}

// Start initiates discovery and launches the real-time etcd watch loop
func (p *UpstreamPool) Start(ctx context.Context) error {
	if err := p.refresh(ctx); err != nil {
		log.Printf("[%s] Warning: initial upstream discovery failed: %v", constants.ServiceNameLoadBalancer, err)
	}

	go p.watch(ctx)
	return nil
}

func (p *UpstreamPool) refresh(ctx context.Context) error {
	resp, err := p.client.Get(ctx, constants.EtcdPrefixConnServers, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	newTargets := make([]*url.URL, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		addr := string(kv.Value)
		if addr == "" {
			continue
		}
		if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
			addr = "http://" + addr
		}
		if u, err := url.Parse(addr); err == nil {
			newTargets = append(newTargets, u)
		}
	}

	p.mu.Lock()
	p.targets = newTargets
	p.mu.Unlock()

	log.Printf("[%s] Dynamic Upstream Pool updated (%d healthy Connection Servers): %v",
		constants.ServiceNameLoadBalancer, len(newTargets), newTargets)
	return nil
}

func (p *UpstreamPool) watch(ctx context.Context) {
	for {
		select {
		case <-p.stopChan:
			return
		case <-ctx.Done():
			return
		default:
		}

		rch := p.client.Watch(ctx, constants.EtcdPrefixConnServers, clientv3.WithPrefix())
		for {
			select {
			case <-p.stopChan:
				return
			case <-ctx.Done():
				return
			case wresp, ok := <-rch:
				if !ok {
					log.Printf("[%s] etcd watch channel closed, reconnecting in 1s...", constants.ServiceNameLoadBalancer)
					select {
					case <-p.stopChan:
						return
					case <-time.After(1 * time.Second):
					}
					goto Reconnect
				}
				if wresp.Err() != nil {
					continue
				}

				refCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				_ = p.refresh(refCtx)
				cancel()
			}
		}
	Reconnect:
	}
}

// Next returns the next healthy upstream Connection Server using round-robin balancing
func (p *UpstreamPool) Next() (*url.URL, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.targets) == 0 {
		return nil, fmt.Errorf("no healthy connection servers currently available in pool")
	}

	idx := atomic.AddUint64(&p.counter, 1) % uint64(len(p.targets))
	return p.targets[idx], nil
}

// Targets returns a list of healthy upstream server URLs
func (p *UpstreamPool) Targets() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	res := make([]string, 0, len(p.targets))
	for _, t := range p.targets {
		res = append(res, t.String())
	}
	return res
}

// Stop cleanly terminates the etcd watcher and connection
func (p *UpstreamPool) Stop() {
	close(p.stopChan)
	_ = p.client.Close()
}
