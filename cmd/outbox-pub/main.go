package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/twmb/franz-go/pkg/kgo"

	"feature-flag-service/internal/constants"
)

type Config struct {
	DatabaseURL string
	KafkaBroker string
}

func loadConfig() Config {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5440/featureflags?sslmode=disable"
	}
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:19092"
	}
	return Config{
		DatabaseURL: dbURL,
		KafkaBroker: broker,
	}
}

func main() {
	cfg := loadConfig()
	log.Printf("[%s] Starting Pure Go Log-Based CDC Publisher...", constants.ServiceNameOutboxPub)
	log.Printf("[%s] Connecting to message broker at %s", constants.ServiceNameOutboxPub, cfg.KafkaBroker)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// 1. Initialize Kafka (Redpanda) Producer
	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.KafkaBroker),
		kgo.DefaultProduceTopic(constants.TopicFlagChanges),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		log.Fatalf("[%s] Failed to initialize Kafka client: %v", constants.ServiceNameOutboxPub, err)
	}
	defer kafkaClient.Close()

	// Test Kafka connection with a ping/metadata fetch
	for {
		if err := kafkaClient.Ping(ctx); err == nil {
			log.Printf("[%s] Connected to Kafka broker successfully.", constants.ServiceNameOutboxPub)
			break
		}
		log.Printf("[%s] Waiting for Kafka broker to be ready...", constants.ServiceNameOutboxPub)
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}

	// 2. Initialize PostgreSQL Replication Connection
	pgConfig, err := pgconn.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("[%s] Failed to parse database config: %v", constants.ServiceNameOutboxPub, err)
	}
	if pgConfig.RuntimeParams == nil {
		pgConfig.RuntimeParams = make(map[string]string)
	}
	pgConfig.RuntimeParams["replication"] = "database"

	var conn *pgconn.PgConn
	for {
		conn, err = pgconn.ConnectConfig(ctx, pgConfig)
		if err == nil {
			log.Printf("[%s] PostgreSQL replication connection established.", constants.ServiceNameOutboxPub)
			break
		}
		log.Printf("[%s] Waiting for PostgreSQL replication connection: %v", constants.ServiceNameOutboxPub, err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
	defer conn.Close(context.Background())

	// 3. Ensure Replication Slot exists
	slotName := constants.ReplicationSlotOutbox
	outputPlugin := constants.LogicalOutputPlugin
	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{
		Temporary:      false,
		SnapshotAction: "NOEXPORT_SNAPSHOT",
	})
	if err != nil {
		// Code 42710 = duplicate_object (slot already exists)
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "42710" {
			log.Printf("[%s] Replication slot '%s' already exists.", constants.ServiceNameOutboxPub, slotName)
		} else {
			log.Fatalf("[%s] Failed to create replication slot: %v", constants.ServiceNameOutboxPub, err)
		}
	} else {
		log.Printf("[%s] Created replication slot '%s'.", constants.ServiceNameOutboxPub, slotName)
	}

	// 4. Start Streaming Logical Replication
	pluginArgs := []string{
		"proto_version '1'",
		fmt.Sprintf("publication_names '%s'", constants.PublicationOutbox),
	}
	err = pglogrepl.StartReplication(ctx, conn, slotName, 0, pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArgs,
	})
	if err != nil {
		log.Fatalf("[%s] Failed to start logical replication: %v", constants.ServiceNameOutboxPub, err)
	}
	log.Printf("[%s] Streaming WAL changes from publication '%s' on slot '%s'...", constants.ServiceNameOutboxPub, constants.PublicationOutbox, slotName)

	// 5. CDC Event Loop
	relations := make(map[uint32]*pglogrepl.RelationMessage)
	lastWrittenLSN := pglogrepl.LSN(0)
	nextStandbyMessageDeadline := time.Now().Add(10 * time.Second)

	for {
		if ctx.Err() != nil {
			log.Printf("[%s] Shutting down CDC publisher gracefully...", constants.ServiceNameOutboxPub)
			break
		}

		// Send periodic heartbeat to keep replication slot alive
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: lastWrittenLSN,
			})
			if err != nil {
				log.Printf("[%s] Failed to send standby status: %v", constants.ServiceNameOutboxPub, err)
			}
			nextStandbyMessageDeadline = time.Now().Add(10 * time.Second)
		}

		receiveCtx, receiveCancel := context.WithTimeout(ctx, 5*time.Second)
		rawMsg, err := conn.ReceiveMessage(receiveCtx)
		receiveCancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			if ctx.Err() != nil {
				break
			}
			log.Printf("[%s] Error receiving replication message: %v", constants.ServiceNameOutboxPub, err)
			time.Sleep(1 * time.Second)
			continue
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			log.Printf("[%s] PostgreSQL WAL replication error: %s", constants.ServiceNameOutboxPub, errMsg.Message)
			continue
		}

		copyDataMsg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch copyDataMsg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyDataMsg.Data[1:])
			if err != nil {
				log.Printf("[%s] Failed to parse keepalive message: %v", constants.ServiceNameOutboxPub, err)
				continue
			}
			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{} // Force immediate reply
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(copyDataMsg.Data[1:])
			if err != nil {
				log.Printf("[%s] Failed to parse XLogData: %v", constants.ServiceNameOutboxPub, err)
				continue
			}

			logicalMsg, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				log.Printf("[%s] Failed to parse logical message: %v", constants.ServiceNameOutboxPub, err)
				continue
			}

			switch msg := logicalMsg.(type) {
			case *pglogrepl.RelationMessage:
				relations[msg.RelationID] = msg

			case *pglogrepl.InsertMessage:
				rel, exists := relations[msg.RelationID]
				if !exists {
					log.Printf("[%s] Received insert for unknown relation ID %d", constants.ServiceNameOutboxPub, msg.RelationID)
					continue
				}

				// Extract env_id and payload columns from the WAL tuple
				var envID string
				var payloadJSON []byte
				for i, col := range msg.Tuple.Columns {
					if i >= len(rel.Columns) {
						break
					}
					colName := rel.Columns[i].Name
					colValue := string(col.Data)

					if strings.EqualFold(colName, "env_id") {
						envID = colValue
					} else if strings.EqualFold(colName, "payload") {
						payloadJSON = col.Data
					}
				}

				if len(payloadJSON) > 0 {
					log.Printf("[%s] [CDC WAL Event] Captured Outbox Insert: env_id=%s, payload_len=%d",
						constants.ServiceNameOutboxPub, envID, len(payloadJSON))

					// Publish to Kafka topic 'flag-changes' partitioned by env_id
					record := &kgo.Record{
						Topic: constants.TopicFlagChanges,
						Key:   []byte(envID),
						Value: payloadJSON,
					}

					var produceErr error
					for retries := 0; retries < 5; retries++ {
						results := kafkaClient.ProduceSync(ctx, record)
						produceErr = results.FirstErr()
						if produceErr == nil {
							log.Printf("[%s] Successfully published WAL event to Kafka topic '%s' (partition %d, offset %d)",
								constants.ServiceNameOutboxPub, constants.TopicFlagChanges, record.Partition, record.Offset)
							break
						}
						log.Printf("[%s] Kafka produce attempt %d/5 failed: %v", constants.ServiceNameOutboxPub, retries+1, produceErr)
						time.Sleep(500 * time.Millisecond)
					}

					if produceErr != nil {
						log.Printf("[%s] CRITICAL: Dropping or halting on unpublishable event: %v", constants.ServiceNameOutboxPub, produceErr)
						continue
					}
				}

				lastWrittenLSN = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			}
		}
	}
}
