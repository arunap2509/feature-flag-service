package constants

// HTTP Headers
const (
	HeaderIfMatch       = "If-Match"
	HeaderETag          = "ETag"
	HeaderUser          = "X-User"
	HeaderContentType   = "Content-Type"
	HeaderXForwardedFor = "X-Forwarded-For"
	ContentTypeJSON     = "application/json"
	HealthzPath         = "/healthz"
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
	ServiceNameMgmtAPI      = "management-api"
	ServiceNameOutboxPub    = "outbox-cdc-publisher"
	ServiceNameEvaluator    = "evaluator"
	ServiceNameConnServer   = "conn-server"
	ServiceNameLoadBalancer = "load-balancer"
	StatusHealthy           = "ok"
	StatusDegraded          = "degraded"
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
	TopicFlagChanges                  = "flag-changes"
	TopicDeliveries                   = "deliveries"
	KafkaGroupEvaluator               = "evaluator-cluster"
	DefaultKafkaBroker                = "redpanda-control:9092"
	DefaultKafkaControlBroker         = "redpanda-control:9092"
	DefaultKafkaDeliveriesBroker      = "redpanda-deliveries:9092"
	DefaultLocalKafkaControlBroker    = "localhost:19092"
	DefaultLocalKafkaDeliveriesBroker = "localhost:19093"
)

// Distributed Coordination (etcd)
const (
	DefaultEtcdEndpoints  = "etcd:2379"
	EtcdPrefixEvaluators  = "/members/evaluators/"
	EtcdPrefixConnServers = "/members/conn-servers/"
	DefaultEtcdLeaseTTL   = 3
	DefaultConnServerPort = "8091"
	DefaultLoadBalancerPort = "8080"
	DefaultDeliveriesPartitions = 8
	DefaultKeepaliveIntervalSec = 15
)

// Server-Sent Events (SSE) Protocol
const (
	HeaderLastEventID = "Last-Event-ID"
	SSEEventPut       = "put"
	SSEEventPatch     = "patch"
	SSEEventPing      = "ping"
)

// Context Store (Redis)
const (
	DefaultRedisAddr      = "redis:6379"
	DefaultLocalRedisAddr = "localhost:6379"
	RedisPrefixShard      = "shard:"
)

// Database & Local Connection Defaults
const (
	DefaultLocalPostgresURL = "postgres://postgres:postgres@localhost:5440/featureflags?sslmode=disable"
	DefaultDockerPostgresURL = "postgres://postgres:postgres@postgres:5432/featureflags?sslmode=disable"
)

// Query Parameters & Routing
const (
	QueryParamEnvID      = "env_id"
	QueryParamUserID     = "user_id"
	QueryParamClientType = "client_type"
)

// Simulation & Benchmarking Constants
const (
	ScenarioMixed           = "mixed"
	ScenarioRollout         = "rollout"
	ScenarioToggle          = "toggle"
	ScenarioKill            = "kill"
	ClientModeDual          = "dual"
	ClientModeMobile        = "mobile"
	ClientModeServer        = "server"
	SegmentBetaTesters      = "beta_testers"
	SegmentEnterpriseTier   = "enterprise_tier"
	DefaultOperatorRate     = 20
	DefaultMobileClients    = 500
	DefaultServerClients    = 20
	DefaultGatewayURL       = "http://localhost:8080"
	DefaultMgmtAPIURL       = "http://localhost:8081"
	DefaultK8sGatewayURL    = "http://localhost:18080"
	DefaultK8sMgmtAPIURL    = "http://localhost:18081"
	DefaultK8sNamespace     = "feature-flags"
	DefaultFleetWatchPort   = "9090"
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
	ErrMsgNoUpstreamsAvailable    = "no healthy connection servers available"
	ErrMsgUpstreamConnectionError = "upstream connection error"
)
