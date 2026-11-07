package deliveries

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"

	"feature-flag-service/internal/connserver/registry"
	"feature-flag-service/internal/constants"
	"feature-flag-service/internal/evaluator/session"
)

// DeliveryBatch represents a batch of diff patches sent from an Evaluator to a Connection Server slot
type DeliveryBatch struct {
	SlotID     int                 `json:"slot_id"`
	EnvID      string              `json:"env_id"`
	EnvVersion int64               `json:"env_version"`
	Patches    []session.DiffPatch `json:"patches"`
}

// Consumer consumes slot-specific mailbox partitions from the Kafka deliveries topic
type Consumer struct {
	client    *kgo.Client
	slotID    int
	partition int32
	registry  *registry.Registry
}

// NewConsumer initializes a partition consumer for the connection server's slot
func NewConsumer(broker string, slotID int, reg *registry.Registry) (*Consumer, error) {
	partition := int32((slotID - 1) % constants.DefaultDeliveriesPartitions)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			constants.TopicDeliveries: {
				partition: kgo.NewOffset().AtEnd(),
			},
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create deliveries kafka client: %w", err)
	}

	return &Consumer{
		client:    client,
		slotID:    slotID,
		partition: partition,
		registry:  reg,
	}, nil
}

// Start runs the Kafka poll loop in a background goroutine
func (c *Consumer) Start(ctx context.Context) {
	log.Printf("[%s] Deliveries consumer active for slot %d (consuming partition %d)...",
		constants.ServiceNameConnServer, c.slotID, c.partition)

	for {
		if ctx.Err() != nil {
			return
		}

		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				log.Printf("[%s] Deliveries poll error: %v", constants.ServiceNameConnServer, err.Err)
			}
			continue
		}

		fetches.EachRecord(func(record *kgo.Record) {
			var batch DeliveryBatch
			if err := json.Unmarshal(record.Value, &batch); err != nil {
				log.Printf("[%s] Malformed delivery batch on partition %d: %v",
					constants.ServiceNameConnServer, c.partition, err)
				return
			}

			dispatchedCount := 0
			for _, patch := range batch.Patches {
				conn, ok := c.registry.Get(patch.ConnectionID)
				if !ok {
					continue
				}

				patchPayload, err := json.Marshal(map[string]any{
					"type":                constants.SSEEventPatch,
					"connection_id":       patch.ConnectionID,
					"environment_version": patch.EnvVersion,
					"changes":             patch.Changes,
				})
				if err != nil {
					continue
				}

				select {
				case conn.PatchChan <- patchPayload:
					dispatchedCount++
				default:
					log.Printf("[%s] Warning: client connection %s buffer full, dropping frame",
						constants.ServiceNameConnServer, patch.ConnectionID)
				}
			}

			if dispatchedCount > 0 {
				log.Printf("[%s] Slot %d: dispatched %d patches (batch total: %d, env_version: %d)",
					constants.ServiceNameConnServer, c.slotID, dispatchedCount, len(batch.Patches), batch.EnvVersion)
			}
		})
	}
}

// Close terminates the deliveries kafka client
func (c *Consumer) Close() {
	c.client.Close()
}
