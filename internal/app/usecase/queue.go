package usecase

import (
	"context"

	"github.com/MateoRamirezRubio1/project_MOM/internal/domain/model"
	"github.com/MateoRamirezRubio1/project_MOM/internal/ports/inbound"
	"github.com/MateoRamirezRubio1/project_MOM/internal/ports/outbound"
	"github.com/google/uuid"
)

type queueUC struct {
	meta outbound.MetaStore
	msg  outbound.MessageStore
}

func NewQueue(meta outbound.MetaStore, msg outbound.MessageStore) inbound.Queue {
	return &queueUC{meta: meta, msg: msg}
}

func (q *queueUC) CreateQueue(ctx context.Context, name, user string) error {
	if err := q.meta.CreateQueue(ctx, name, user); err != nil {
		return err
	}
	// crea stub vacío para garantizar existencia en msgStore
	return q.msg.Enqueue(ctx, name, model.Message{})
}

func (q *queueUC) ListQueues(ctx context.Context) ([]string, error) {
	return q.meta.ListQueues(ctx)
}

func (q *queueUC) DeleteQueue(ctx context.Context, name, user string) error {
	return q.meta.DeleteQueue(ctx, name, user)
}

func (q *queueUC) Enqueue(ctx context.Context, queue, payload, user string) error {
	m := model.Message{
		ID:       uuid.New(),
		Payload:  []byte(payload),
		Producer: user,
	}
	return q.msg.Enqueue(ctx, queue, m)
}

func (q *queueUC) Dequeue(ctx context.Context, queue string) (*model.Message, error) {
	return q.msg.Dequeue(ctx, queue)
}

func (q *queueUC) Ack(ctx context.Context, queue string, id uuid.UUID) error {
	return q.msg.Ack(ctx, queue, id)
}
