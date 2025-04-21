package inbound

import "context"

type Admin interface {
	CreateTopic(ctx context.Context, name string, partitions int, user string) error
	ListTopics(ctx context.Context) ([]string, error)
}
