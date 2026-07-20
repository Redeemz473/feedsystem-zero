package eventx

const (
	OutboxStatusPending    = 1
	OutboxStatusSent       = 2
	OutboxStatusFailed     = 3
	OutboxStatusDead       = 4
	OutboxStatusProcessing = 5
)

const (
	DefaultOutboxBatchSize         = 100
	DefaultProcessedEventTTLDays   = 14
	DefaultInteractionEventTTLDays = 30
)
