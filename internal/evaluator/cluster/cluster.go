package cluster

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"feature-flag-service/internal/constants"
	"feature-flag-service/internal/evaluator/ring"
)

// Coordinator manages evaluator cluster membership and lease tracking via etcd
type Coordinator struct {
	client    *clientv3.Client
	nodeID    string
	nodeAddr  string
	ring      *ring.Ring
	leaseID   clientv3.LeaseID
	revision  int64
	nodeAddrs map[string]string
	mu        sync.RWMutex
	stopChan  chan struct{}
	leaseTTL  int64
}

// NewCoordinator creates a new cluster coordinator connected to etcd
func NewCoordinator(endpoints []string, nodeID, nodeAddr string, hashRing *ring.Ring, leaseTTL int64) (*Coordinator, error) {
	if leaseTTL <= 0 {
		leaseTTL = constants.DefaultEtcdLeaseTTL
	}
	if nodeAddr == "" {
		nodeAddr = nodeID
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd: %w", err)
	}

	return &Coordinator{
		client:    cli,
		nodeID:    nodeID,
		nodeAddr:  nodeAddr,
		ring:      hashRing,
		nodeAddrs: make(map[string]string),
		stopChan:  make(chan struct{}),
		leaseTTL:  leaseTTL,
	}, nil
}

// Start registers the node with etcd and starts resilient lease maintenance and watch loops
func (c *Coordinator) Start(ctx context.Context) error {
	// 1. Initial registration before declaring node ready
	if err := c.registerOnce(ctx); err != nil {
		return fmt.Errorf("initial etcd registration failed: %w", err)
	}

	// 2. Start resilient background lease maintainer (auto-reconnects on lease drop)
	go c.maintainLease(ctx)

	// 3. Start resilient watch loop for evaluator membership changes
	go c.watchMembers(ctx)

	return nil
}

func (c *Coordinator) registerOnce(ctx context.Context) error {
	grantResp, err := c.client.Grant(ctx, c.leaseTTL)
	if err != nil {
		return fmt.Errorf("failed to create etcd lease: %w", err)
	}

	c.mu.Lock()
	c.leaseID = grantResp.ID
	c.mu.Unlock()

	nodeKey := fmt.Sprintf("%s%s", constants.EtcdPrefixEvaluators, c.nodeID)
	_, err = c.client.Put(ctx, nodeKey, c.nodeAddr, clientv3.WithLease(c.leaseID))
	if err != nil {
		return fmt.Errorf("failed to register evaluator in etcd: %w", err)
	}

	log.Printf("[%s] Node '%s' (%s) registered with etcd (lease %x, TTL %ds)",
		constants.ServiceNameEvaluator, c.nodeID, c.nodeAddr, c.leaseID, c.leaseTTL)

	if err := c.refreshMembers(ctx); err != nil {
		log.Printf("[%s] Warning: member refresh failed: %v", constants.ServiceNameEvaluator, err)
	}
	return nil
}

func (c *Coordinator) maintainLease(ctx context.Context) {
	for {
		select {
		case <-c.stopChan:
			return
		case <-ctx.Done():
			return
		default:
		}

		c.mu.RLock()
		leaseID := c.leaseID
		c.mu.RUnlock()

		keepAliveCh, err := c.client.KeepAlive(ctx, leaseID)
		if err != nil {
			log.Printf("[%s] Warning: etcd keepalive failed: %v, re-registering in 1s...",
				constants.ServiceNameEvaluator, err)
			select {
			case <-c.stopChan:
				return
			case <-time.After(1 * time.Second):
				_ = c.registerOnce(ctx)
				continue
			}
		}

		keepAliveActive := true
		for keepAliveActive {
			select {
			case <-c.stopChan:
				return
			case <-ctx.Done():
				return
			case _, ok := <-keepAliveCh:
				if !ok {
					log.Printf("[%s] Warning: etcd keepalive channel closed, re-establishing lease immediately...",
						constants.ServiceNameEvaluator)
					keepAliveActive = false
				}
			}
		}

		// Channel closed: re-acquire lease and put node key
		regCtx, regCancel := context.WithTimeout(ctx, 3*time.Second)
		if err := c.registerOnce(regCtx); err != nil {
			log.Printf("[%s] Re-registration failed: %v, retrying in 1s...", constants.ServiceNameEvaluator, err)
			select {
			case <-c.stopChan:
				regCancel()
				return
			case <-time.After(1 * time.Second):
			}
		}
		regCancel()
	}
}

func (c *Coordinator) refreshMembers(ctx context.Context) error {
	resp, err := c.client.Get(ctx, constants.EtcdPrefixEvaluators, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	nodeIDs := make([]string, 0, len(resp.Kvs))
	nodeAddrs := make(map[string]string)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		nodeID := strings.TrimPrefix(key, constants.EtcdPrefixEvaluators)
		if nodeID != "" {
			nodeIDs = append(nodeIDs, nodeID)
			nodeAddrs[nodeID] = string(kv.Value)
		}
	}

	c.mu.Lock()
	c.revision = resp.Header.Revision
	c.nodeAddrs = nodeAddrs
	c.mu.Unlock()

	c.ring.SetMembers(nodeIDs)
	log.Printf("[%s] Cluster ring updated (revision %d). Active nodes (%d): %v",
		constants.ServiceNameEvaluator, resp.Header.Revision, len(nodeIDs), nodeIDs)
	return nil
}

// GetMemberAddr returns the advertised network address for an evaluator node ID
func (c *Coordinator) GetMemberAddr(nodeID string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nodeAddrs[nodeID]
}

func (c *Coordinator) watchMembers(ctx context.Context) {
	for {
		select {
		case <-c.stopChan:
			return
		case <-ctx.Done():
			return
		default:
		}

		rch := c.client.Watch(ctx, constants.EtcdPrefixEvaluators, clientv3.WithPrefix())
		for {
			select {
			case <-c.stopChan:
				return
			case <-ctx.Done():
				return
			case wresp, ok := <-rch:
				if !ok {
					log.Printf("[%s] etcd watch channel closed, re-establishing watcher in 1s...", constants.ServiceNameEvaluator)
					select {
					case <-c.stopChan:
						return
					case <-time.After(1 * time.Second):
					}
					goto ReconnectWatch
				}
				if wresp.Err() != nil {
					log.Printf("[%s] etcd watch error: %v", constants.ServiceNameEvaluator, wresp.Err())
					continue
				}

				// On any membership change event, refresh entire member list
				refreshCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				if err := c.refreshMembers(refreshCtx); err != nil {
					log.Printf("[%s] Error refreshing members after watch event: %v", constants.ServiceNameEvaluator, err)
				}
				cancel()
			}
		}
	ReconnectWatch:
	}
}

// Revision returns the current cluster revision number
func (c *Coordinator) Revision() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.revision
}

// Stop revokes the lease and closes the etcd client connection
func (c *Coordinator) Stop() {
	close(c.stopChan)
	if c.leaseID != 0 {
		revokeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = c.client.Revoke(revokeCtx, c.leaseID)
		cancel()
	}
	_ = c.client.Close()
	log.Printf("[%s] Node '%s' cleanly deregistered from etcd", constants.ServiceNameEvaluator, c.nodeID)
}
