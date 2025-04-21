package outbound

import (
	"context"

	"github.com/MateoRamirezRubio1/project_MOM/internal/domain/model"
	"github.com/google/uuid"
)

type MessageStore interface {
	// tópicos ------------------------------------------------------
	Append(ctx context.Context, msg model.Message) (offset uint64, err error)
	Read(ctx context.Context, topic string, part int, from uint64, max int) ([]model.Message, error)
	Delete(ctx context.Context, topic string, part int, offset uint64) error // sin uso aún

	// colas --------------------------------------------------------
	Enqueue(ctx context.Context, queue string, msg model.Message) error
	Dequeue(ctx context.Context, queue string) (*model.Message, error)
	Ack(ctx context.Context, queue string, id uuid.UUID) error
}
