package outbound

import "context"

type MetaStore interface {
	CreateTopic(ctx context.Context, name string, partitions int, creator string) error
	GetTopic(ctx context.Context, name string) (partitions int, err error)
	ListTopics(ctx context.Context) ([]string, error)
	GetOffset(ctx context.Context, group string, topic string, part int) (uint64, error)
	CommitOffset(ctx context.Context, group string, topic string, part int, off uint64) error
}
