package outbound

import "context"

// MetaStore almacena metadatos de tópicos, colas y offsets.
type MetaStore interface {
	// ­­­­­­­­­­­­­ TOPICS ­­­­­­­­­­­­
	CreateTopic(ctx context.Context, name string, partitions int, creator string) error
	GetTopic(ctx context.Context, name string) (partitions int, err error)
	ListTopics(ctx context.Context) ([]string, error)
	DeleteTopic(ctx context.Context, name, user string) error

	// ­­­­­­­­­­­­­ QUEUES ­­­­­­­­­­­­
	CreateQueue(ctx context.Context, name, creator string) error
	ListQueues(ctx context.Context) ([]string, error)
	DeleteQueue(ctx context.Context, name, user string) error

	// ­­­­­­­­­­­­­ OFFSETS (consumer groups) ­­­­­­­­­­­­
	GetOffset(ctx context.Context, group, topic string, part int) (uint64, error)
	CommitOffset(ctx context.Context, group, topic string, part int, offset uint64) error
}
