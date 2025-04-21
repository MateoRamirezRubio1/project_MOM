package usecase

import (
	"context"

	"github.com/MateoRamirezRubio1/project_MOM/internal/domain/model"
	"github.com/MateoRamirezRubio1/project_MOM/internal/domain/service"
	"github.com/MateoRamirezRubio1/project_MOM/internal/ports/inbound"
	"github.com/MateoRamirezRubio1/project_MOM/internal/ports/outbound"
)

type publisherUC struct {
	meta outbound.MetaStore
	msg  outbound.MessageStore
	auth outbound.AuthStore
}

func NewPublisher(meta outbound.MetaStore, msg outbound.MessageStore, auth outbound.AuthStore) inbound.Publisher {
	return &publisherUC{meta: meta, msg: msg, auth: auth}
}

func (p *publisherUC) Publish(ctx context.Context, topic, key, payload, user string) (int, uint64, error) {
	parts, err := p.meta.GetTopic(ctx, topic)
	if err != nil {
		return 0, 0, err
	}
	partID := service.HashPartition(key, parts)
	msg := model.Message{
		Topic:    topic,
		PartID:   partID,
		Key:      key,
		Payload:  []byte(payload),
		Producer: user,
	}
	offset, err := p.msg.Append(ctx, msg)
	return partID, offset, err
}
