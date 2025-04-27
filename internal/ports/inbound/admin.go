package inbound

import "context"

// Admin expone todas las operaciones de gestión (tópicos y colas).
type Admin interface {
	// Tópicos
	CreateTopic(ctx context.Context, name string, partitions int, user string) error
	ListTopics(ctx context.Context) ([]string, error)
	DeleteTopic(ctx context.Context, name, user string) error

	// Colas
	CreateQueue(ctx context.Context, name, user string) error
	ListQueues(ctx context.Context) ([]string, error)
	DeleteQueue(ctx context.Context, name, user string) error
}
