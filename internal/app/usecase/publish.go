package usecase

import (
	"context"

	"github.com/MateoRamirezRubio1/project_MOM/internal/cluster"
	cl "github.com/MateoRamirezRubio1/project_MOM/internal/cluster"
	pb "github.com/MateoRamirezRubio1/project_MOM/internal/clusterpb"
	"github.com/MateoRamirezRubio1/project_MOM/internal/domain/model"
	"github.com/MateoRamirezRubio1/project_MOM/internal/domain/service"
	"github.com/MateoRamirezRubio1/project_MOM/internal/ports/inbound"
	"github.com/MateoRamirezRubio1/project_MOM/internal/ports/outbound"
	"github.com/google/uuid"
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

	// --- construye con UUID antes de Append ---
	m := model.Message{
		ID:       uuid.New(),
		Topic:    topic,
		PartID:   partID,
		Key:      key,
		Payload:  []byte(payload),
		Producer: user,
	}
	offset, err := p.msg.Append(ctx, m)
	if err != nil {
		return 0, 0, err
	}
	cluster.TrackNextOffset(topic, partID, offset+1)

	// registro HWM para reconciliación ----
	cl.TrackNextOffset(topic, partID, offset+1)

	// ---- fan-out a peers (si se está en cluster) -------
	if p.fan != nil {
		p.fan.Broadcast(ctx, []*pb.Message{{
			Uuid:    m.ID.String(),
			Topic:   topic,
			Part:    uint32(partID),
			Offset:  offset,
			Key:     key,
			User:    user,
			Payload: []byte(payload),
		}})
	}
	return partID, offset, nil
}
