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

func (c *consumerUC) Pull(ctx context.Context, topic string, part int, from uint64, max int) ([]model.Message, error) {
	// validar partición existente
	partitions, err := c.meta.GetTopic(ctx, topic)
	if err != nil {
		return nil, err
	}
	if part >= partitions {
		return nil, errors.New("partition out of range")
	}
	return c.msg.Read(ctx, topic, part, from, max)
}
