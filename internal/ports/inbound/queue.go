package inbound

import (
	"context"

	"github.com/MateoRamirezRubio1/project_MOM/internal/domain/model"
	"github.com/google/uuid"
)

type Queue interface {
	CreateQueue(ctx context.Context, name, creator string) error
	Enqueue(ctx context.Context, queue string, payload string, producer string) error
	Dequeue(ctx context.Context, queue string) (*model.Message, error)
	Ack(ctx context.Context, queue string, id uuid.UUID) error
}
