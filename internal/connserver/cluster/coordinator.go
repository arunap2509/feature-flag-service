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

// Coordinator manages slot lease reservation and evaluator discovery via etcd
type Coordinator struct {
	client         *clientv3.Client
	slotID         int
	serverAddr     string
	ring           *ring.Ring
	leaseID        clientv3.LeaseID
	evaluatorAddrs map[string]string
	revision       int64
	mu             sync.RWMutex
	stopChan       chan struct{}
	leaseTTL       int64
}

// NewCoordinator creates a new cluster coordinator for the connection server
func NewCoordinator(endpoints []string, slotID int, serverAddr string, hashRing *ring.Ring, leaseTTL int64) (*Coordinator, error) {
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
		client:         cli,
		slotID:         slotID,
		serverAddr:     serverAddr,
		ring:           hashRing,
		evaluatorAddrs: make(map[string]string),
		stopChan:       make(chan struct{}),
		leaseTTL:       leaseTTL,
	}, nil
}

// Start claims the slot key in etcd and starts the lease maintainer and evaluator watcher
func (c *Coordinator) Start(ctx context.Context) error {
	if err := c.claimSlotOnce(ctx); err != nil {
		return fmt.Errorf("initial slot claim failed: %w", err)
	}

	// 1. Maintain slot lease keepalive
	go c.maintainSlotLease(ctx)

	// 2. Discover existing evaluators
	if err := c.refreshEvaluators(ctx); err != nil {
		log.Printf("[%s] Warning: initial evaluator discovery failed: %v", constants.ServiceNameConnServer, err)
	}

	// 3. Watch for evaluator ring membership changes
	go c.watchEvaluators(ctx)

	return nil
}

func (c *Coordinator) claimSlotOnce(ctx context.Context) error {
	grantResp, err := c.client.Grant(ctx, c.leaseTTL)
	if err != nil {
		return fmt.Errorf("failed to create etcd lease: %w", err)
	}

	c.mu.Lock()
	c.leaseID = grantResp.ID
	c.mu.Unlock()

	slotKey := fmt.Sprintf("%sslot-%d", constants.EtcdPrefixConnServers, c.slotID)

	// Atomic Put with lease
	_, err = c.client.Put(ctx, slotKey, c.serverAddr, clientv3.WithLease(c.leaseID))
	if err != nil {
		return fmt.Errorf("failed to claim slot %d in etcd: %w", c.slotID, err)
	}

	log.Printf("[%s] Slot %d successfully claimed in etcd (lease %x, TTL %ds, addr: %s)",
		constants.ServiceNameConnServer, c.slotID, c.leaseID, c.leaseTTL, c.serverAddr)
	return nil
}

func (c *Coordinator) maintainSlotLease(ctx context.Context) {
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
			log.Printf("[%s] Warning: slot %d lease keepalive failed: %v, re-registering in 1s...",
				constants.ServiceNameConnServer, c.slotID, err)
			select {
			case <-c.stopChan:
				return
			case <-time.After(1 * time.Second):
				_ = c.claimSlotOnce(ctx)
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
					log.Printf("[%s] Warning: slot %d keepalive channel closed, re-establishing lease immediately...",
						constants.ServiceNameConnServer, c.slotID)
					keepAliveActive = false
				}
			}
		}

		regCtx, regCancel := context.WithTimeout(ctx, 3*time.Second)
		_ = c.claimSlotOnce(regCtx)
		regCancel()
	}
}

func (c *Coordinator) refreshEvaluators(ctx context.Context) error {
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
			addr := string(kv.Value)
			if addr == "" {
				addr = fmt.Sprintf("http://%s:9001", nodeID)
			}
			nodeAddrs[nodeID] = addr
		}
	}

	c.mu.Lock()
	c.revision = resp.Header.Revision
	c.evaluatorAddrs = nodeAddrs
	c.mu.Unlock()

	c.ring.SetMembers(nodeIDs)
	log.Printf("[%s] Discovered %d evaluator nodes on ring. Addrs: %v",
		constants.ServiceNameConnServer, len(nodeIDs), nodeAddrs)
	return nil
}

func (c *Coordinator) watchEvaluators(ctx context.Context) {
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
					log.Printf("[%s] Evaluator watch channel closed, reconnecting...", constants.ServiceNameConnServer)
					select {
					case <-c.stopChan:
						return
					case <-time.After(1 * time.Second):
					}
					goto ReconnectWatch
				}
				if wresp.Err() != nil {
					log.Printf("[%s] Evaluator watch error: %v", constants.ServiceNameConnServer, wresp.Err())
					continue
				}

				refreshCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				_ = c.refreshEvaluators(refreshCtx)
				cancel()
			}
		}
	ReconnectWatch:
	}
}

// GetEvaluatorForShard returns the HTTP base URL of the Evaluator that owns the given environment shard
func (c *Coordinator) GetEvaluatorForShard(envID string, shardID int) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	shardKey := fmt.Sprintf("%s:shard:%d", envID, shardID)
	ownerID, exists := c.ring.GetOwner(shardKey)
	if !exists {
		return "", fmt.Errorf("no active evaluator found on ring")
	}

	addr, ok := c.evaluatorAddrs[ownerID]
	if !ok || addr == "" {
		return fmt.Sprintf("http://%s:9001", ownerID), nil
	}
	return addr, nil
}

// Stop cleanly revokes the slot lease and closes the etcd connection
func (c *Coordinator) Stop() {
	close(c.stopChan)
	if c.leaseID != 0 {
		revokeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = c.client.Revoke(revokeCtx, c.leaseID)
		cancel()
	}
	_ = c.client.Close()
	log.Printf("[%s] Slot %d cleanly deregistered from etcd", constants.ServiceNameConnServer, c.slotID)
}
