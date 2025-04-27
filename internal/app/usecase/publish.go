package usecase

import (
	"context"

	cl "github.com/MateoRamirezRubio1/project_MOM/internal/cluster"
	pb "github.com/MateoRamirezRubio1/project_MOM/internal/clusterpb"
	"github.com/MateoRamirezRubio1/project_MOM/internal/domain/model"
	"github.com/MateoRamirezRubio1/project_MOM/internal/domain/service"
	"github.com/MateoRamirezRubio1/project_MOM/internal/ports/inbound"
	"github.com/MateoRamirezRubio1/project_MOM/internal/ports/outbound"
)

type publisherUC struct {
	meta outbound.MetaStore
	msg  outbound.MessageStore
	auth outbound.AuthStore
	fan  *cl.Fanout // nil si ejecuto single-node
}

func NewPublisher(meta outbound.MetaStore, msg outbound.MessageStore,
	auth outbound.AuthStore, fan *cl.Fanout) inbound.Publisher {

	return &publisherUC{meta: meta, msg: msg, auth: auth, fan: fan}
}

// --------------------------------------------------------------------
// Publish
// --------------------------------------------------------------------

func (p *publisherUC) Publish(ctx context.Context,
	topic, key, payload, user string) (int, uint64, error) {

	parts, err := p.meta.GetTopic(ctx, topic)
	if err != nil {
		return 0, 0, err
	}
	partID := service.HashPartition(key, parts)

	m := model.Message{
		Topic: topic, PartID: partID, Key: key,
		Payload: []byte(payload), Producer: user,
	}
	offset, err := p.msg.Append(ctx, m)
	if err == nil && p.fan != nil {
		p.fan.Broadcast(ctx, []*pb.Message{{
			Topic: topic,
			Part:  uint32(partID),
			Json:  []byte(payload),
		}})
	}
	return partID, offset, err
}
