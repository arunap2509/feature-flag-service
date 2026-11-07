package constants

// HTTP Headers
const (
	HeaderIfMatch     = "If-Match"
	HeaderETag        = "ETag"
	HeaderUser        = "X-User"
	HeaderContentType = "Content-Type"
	ContentTypeJSON   = "application/json"
)

// Flag States
const (
	FlagStateOn  = "on"
	FlagStateOff = "off"
)

// SDK Key Types
const (
	KeyTypeClient = "client"
	KeyTypeServer = "server"
)

// Resource ID Prefixes (Stripe-style sortable identifiers)
const (
	PrefixTenant      = "ten_"
	PrefixEnvironment = "env_"
	PrefixSDKClient   = "sdk_client_"
	PrefixSDKServer   = "sdk_server_"
)

// ID & Cryptographic Parameters
const (
	DefaultIDRandomBytes      = 8
	DefaultSaltByteLength     = 8
	DefaultKeyTokenByteLength = 16
)

// Architecture Defaults
const (
	DefaultShardCount         = 1
	DefaultEnvironmentVersion = 1
	DefaultFlagVersion        = 1
	DefaultPort               = "8081"
	DefaultEvaluatorPort      = "9001"
	DefaultVirtualNodes       = 100
	RolloutBucketModulus      = 100000
	RolloutMultiplier         = 1000
	DefaultOutboxLimit        = 25
)

// Service Metadata
const (
	ServiceNameMgmtAPI   = "management-api"
	ServiceNameOutboxPub = "outbox-cdc-publisher"
	ServiceNameEvaluator = "evaluator"
	StatusHealthy        = "ok"
)

// Transactional Outbox & Logical Replication (CDC)
const (
	TableOutbox            = "outbox"
	PublicationOutbox      = "outbox_pub"
	ReplicationSlotOutbox  = "outbox_cdc_slot"
	LogicalOutputPlugin    = "pgoutput"
	DefaultReplicationHost = "postgres:5432"
)

// Domain Event Types
const (
	EventTypeFlagCreated    = "FLAG_CREATED"
	EventTypeFlagUpdated    = "FLAG_UPDATED"
	EventTypeFlagKilled     = "FLAG_KILLED"
	EventTypeFlagRolledBack = "FLAG_ROLLED_BACK"
	EventTypeSegmentCreated = "SEGMENT_CREATED"
	EventTypeSegmentUpdated = "SEGMENT_UPDATED"
)

// Message Broker (Kafka / Redpanda)
const (
	TopicFlagChanges      = "flag-changes"
	TopicDeliveries       = "deliveries"
	KafkaGroupEvaluator   = "evaluator-cluster"
	DefaultKafkaBroker    = "redpanda:9092"
)

// Distributed Coordination (etcd)
const (
	DefaultEtcdEndpoints = "etcd:2379"
	EtcdPrefixEvaluators = "/members/evaluators/"
	DefaultEtcdLeaseTTL  = 3
)

// Error Messages
const (
	ErrMsgCallerIdentityRequired  = "caller identity is required in X-User header"
	ErrMsgInvalidRequestBody      = "invalid request body"
	ErrMsgInvalidTargetVersion    = "invalid target version"
	ErrMsgFlagKeyRequired         = "flag key is required"
	ErrMsgTenantNameRequired      = "tenant name is required"
	ErrMsgEnvironmentNameRequired = "environment name is required"
	ErrMsgSegmentKeyRequired      = "segment key is required"
	ErrMsgInvalidFlagState        = "state must be 'on' or 'off'"
	ErrMsgInvalidKeyType          = "key_type must be either 'client' or 'server'"
	ErrMsgInvalidIfMatch          = "invalid If-Match header version"
	ErrMsgTargetVersionPositive   = "target version must be greater than 0"
	ErrMsgRecordNotFound          = "record not found"
	ErrMsgVersionMismatch         = "flag version mismatch (please reload)"
	ErrMsgSegmentVersionMismatch  = "segment version mismatch (please reload)"
	ErrMsgInvalidRolloutPct       = "rollout_percentage must be between 0 and 100"
	ErrMsgInvalidRuleOrder        = "rule order must be greater than 0"
	ErrMsgInvalidRuleDefinition   = "rule must specify either a segment or a rollout percentage"
)
