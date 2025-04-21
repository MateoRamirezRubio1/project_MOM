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
	auth outbound.AuthStore
}

func NewQueue(meta outbound.MetaStore, msg outbound.MessageStore, auth outbound.AuthStore) inbound.Queue {
	return &queueUC{meta: meta, msg: msg, auth: auth}
}

// -- MVP: metaStore aún no persiste colas. Creamos in-mem en MessageStore

func (q *queueUC) CreateQueue(_ context.Context, name, _ string) error {
	// en esta versión, simplemente asegura que la cola exista creando un msgStore entry
	return q.msg.Enqueue(context.Background(), name, model.Message{}) // no-op msg
}

func (q *queueUC) Enqueue(ctx context.Context, queue, payload, user string) error {
	m := model.Message{
		ID:       uuid.New(),
		Payload:  []byte(payload),
		Producer: user,
		// Topic/Part no se usan en cola
	}
	return q.msg.Enqueue(ctx, queue, m)
}

func (q *queueUC) Dequeue(ctx context.Context, queue string) (*model.Message, error) {
	return q.msg.Dequeue(ctx, queue)
}

func (q *queueUC) Ack(ctx context.Context, queue string, id uuid.UUID) error {
	return q.msg.Ack(ctx, queue, id)
}

// (re-queue en memoria sucede en adapter con TTL de 30s)
