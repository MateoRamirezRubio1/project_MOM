package usecase

import (
	"context"
	"errors"

	"github.com/MateoRamirezRubio1/project_MOM/internal/domain/model"
	"github.com/MateoRamirezRubio1/project_MOM/internal/ports/inbound"
	"github.com/MateoRamirezRubio1/project_MOM/internal/ports/outbound"
)

type consumerUC struct {
	meta outbound.MetaStore
	msg  outbound.MessageStore
}

func NewConsumer(meta outbound.MetaStore, msg outbound.MessageStore) inbound.Consumer {
	return &consumerUC{meta: meta, msg: msg}
}

// ---------------- TOPIC PULL -----------------------------

// Pull lee los mensajes de un tópico para un grupo específico.
func (c *consumerUC) Pull(ctx context.Context, topic, group string, part int, max int) ([]model.Message, error) {
	// Verifica que la partición esté en el rango válido.
	parts, err := c.meta.GetTopic(ctx, topic)
	if err != nil {
		return nil, err
	}
	if part >= parts {
		return nil, errors.New("partition out of range")
	}

	// Obtiene el offset para el grupo y partición.
	from, _ := c.meta.GetOffset(ctx, group, topic, part)
	return c.msg.Read(ctx, topic, part, from, max)
}

// ---------------- COMMIT OFFSET --------------------------

// Commit guarda el offset del grupo para una partición.
func (c *consumerUC) Commit(ctx context.Context, topic, group string, part int, offset uint64) error {
	// Guarda el nuevo offset después de procesar los mensajes.
	return c.meta.CommitOffset(ctx, group, topic, part, offset)
}
