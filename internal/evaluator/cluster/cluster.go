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
	client       *clientv3.Client
	nodeID       string
	ring         *ring.Ring
	leaseID      clientv3.LeaseID
	revision     int64
	mu           sync.RWMutex
	stopChan     chan struct{}
	leaseTTL     int64
}

// NewCoordinator creates a new cluster coordinator connected to etcd
func NewCoordinator(endpoints []string, nodeID string, hashRing *ring.Ring, leaseTTL int64) (*Coordinator, error) {
	if leaseTTL <= 0 {
		leaseTTL = constants.DefaultEtcdLeaseTTL
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd: %w", err)
	}

	return &Coordinator{
		client:   cli,
		nodeID:   nodeID,
		ring:     hashRing,
		stopChan: make(chan struct{}),
		leaseTTL: leaseTTL,
	}, nil
}

// Start registers the node with etcd and starts the keep-alive and watch loops
func (c *Coordinator) Start(ctx context.Context) error {
	// 1. Grant Lease
	grantResp, err := c.client.Grant(ctx, c.leaseTTL)
	if err != nil {
		return fmt.Errorf("failed to create etcd lease: %w", err)
	}
	c.leaseID = grantResp.ID

	// 2. Register Node Key with Lease
	nodeKey := fmt.Sprintf("%s%s", constants.EtcdPrefixEvaluators, c.nodeID)
	_, err = c.client.Put(ctx, nodeKey, c.nodeID, clientv3.WithLease(c.leaseID))
	if err != nil {
		return fmt.Errorf("failed to register evaluator in etcd: %w", err)
	}
	log.Printf("[%s] Node '%s' registered with etcd (lease %x, TTL %ds)", constants.ServiceNameEvaluator, c.nodeID, c.leaseID, c.leaseTTL)

	// 3. Initial sync of members
	if err := c.refreshMembers(ctx); err != nil {
		log.Printf("[%s] Warning: initial member refresh failed: %v", constants.ServiceNameEvaluator, err)
	}

	// 4. Start KeepAlive background loop
	keepAliveCh, err := c.client.KeepAlive(ctx, c.leaseID)
	if err != nil {
		return fmt.Errorf("failed to start etcd keepalive: %w", err)
	}

	go func() {
		for {
			select {
			case <-c.stopChan:
				return
			case ka, ok := <-keepAliveCh:
				if !ok {
					log.Printf("[%s] Warning: etcd keepalive channel closed", constants.ServiceNameEvaluator)
					return
				}
				_ = ka
			}
		}
	}()

	// 5. Start Watch loop for evaluator membership changes
	go c.watchMembers(ctx)

	return nil
}

func (c *Coordinator) refreshMembers(ctx context.Context) error {
	resp, err := c.client.Get(ctx, constants.EtcdPrefixEvaluators, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.revision = resp.Header.Revision
	c.mu.Unlock()

	nodeIDs := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		nodeID := strings.TrimPrefix(key, constants.EtcdPrefixEvaluators)
		if nodeID != "" {
			nodeIDs = append(nodeIDs, nodeID)
		}
	}

	c.ring.SetMembers(nodeIDs)
	log.Printf("[%s] Cluster ring updated (revision %d). Active nodes (%d): %v",
		constants.ServiceNameEvaluator, resp.Header.Revision, len(nodeIDs), nodeIDs)
	return nil
}

func (c *Coordinator) watchMembers(ctx context.Context) {
	rch := c.client.Watch(ctx, constants.EtcdPrefixEvaluators, clientv3.WithPrefix())
	for {
		select {
		case <-c.stopChan:
			return
		case wresp, ok := <-rch:
			if !ok {
				log.Printf("[%s] etcd watch channel closed", constants.ServiceNameEvaluator)
				return
			}
			if wresp.Err() != nil {
				log.Printf("[%s] etcd watch error: %v", constants.ServiceNameEvaluator, wresp.Err())
				continue
			}

			// On any membership change event, refresh entire member list
			refreshCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			if err := c.refreshMembers(refreshCtx); err != nil {
				log.Printf("[%s] Error refreshing members after watch event: %v", constants.ServiceNameEvaluator, err)
			}
			cancel()
		}
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
