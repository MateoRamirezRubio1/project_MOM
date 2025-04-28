package cluster

import (
	"context"
	"log"

	pb "github.com/MateoRamirezRubio1/project_MOM/internal/clusterpb"
	"github.com/MateoRamirezRubio1/project_MOM/internal/domain/model"
	"github.com/MateoRamirezRubio1/project_MOM/internal/ports/outbound"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

// Fanout mantiene los clientes gRPC a los peers.
type Fanout struct {
	self  string  // ID propio
	cfg   *Config // referencia de utilidad
	peers map[string]pb.ReplicatorClient
}

// NewFanout devuelve nil si:
//
//   - cfg == nil                       → modo single-node
//   - sólo hay 1 nodo en el JSON       → “ ”
func NewFanout(cfg *Config, selfID string) *Fanout {
	if cfg == nil || len(cfg.Nodes) <= 1 {
		return nil
	}
	f := &Fanout{self: selfID, cfg: cfg, peers: map[string]pb.ReplicatorClient{}}

	for _, n := range cfg.Nodes {
		if n.ID == selfID {
			continue
		}
		cc, err := grpc.Dial(n.Host, grpc.WithInsecure())
		if err != nil {
			log.Printf("[cluster] peer %s: %v", n.ID, err)
			continue
		}
		f.peers[n.ID] = pb.NewReplicatorClient(cc)
	}
	return f
}

/*────────────  publicación normal  ───────────*/

func (f *Fanout) Broadcast(ctx context.Context, batch []*pb.Message) {
	if f == nil {
		return
	}
	req := &pb.ReplicateRequest{Batch: batch}
	for id, cli := range f.peers {
		go func(id string, c pb.ReplicatorClient) {
			if _, err := c.Replicate(ctx, req); err != nil {
				log.Printf("[cluster] peer %s error: %v", id, err)
			}
		}(id, cli)
	}
}

/*────────────  catch-up  ───────────*/

// store.Append se encarga de asignar el offset local;
// offset que llega en el lote sólo se usa para avanzar el HWM.
func (f *Fanout) CatchUp(ctx context.Context, peerID string,
	store outbound.MessageStore, topic string, part int,
	from, to uint64) error {

	if f == nil {
		return nil
	}
	cli := f.peers[peerID]
	if cli == nil {
		return nil
	}

	resp, err := cli.GetRange(ctx, &pb.RangeRequest{
		Topic: topic, Part: uint32(part), From: from, To: to,
	})
	if err != nil {
		return err
	}

	for _, m := range resp.Batch {
		_ = store.AppendWithOffset(ctx, model.Message{
			ID:       uuid.MustParse(m.Uuid),
			Topic:    m.Topic,
			PartID:   int(m.Part),
			Offset:   m.Offset,
			Key:      m.Key,
			Producer: m.User,
			Payload:  m.Payload,
		})
	}
	return nil
}

/*────────────  elección simple de líder  ───────────*/

func (f *Fanout) Leader() string {
	if f == nil {
		return f.self
	}
	for _, n := range f.cfg.Nodes { // orden del JSON
		if n.ID == f.self {
			return n.ID
		}
		if f.peers[n.ID] != nil { // “vive”
			return n.ID
		}
	}
	return f.self
}
