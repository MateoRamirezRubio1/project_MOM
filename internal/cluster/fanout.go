package cluster

import (
	"context"
	"log"

	pb "github.com/MateoRamirezRubio1/project_MOM/internal/clusterpb"
	"google.golang.org/grpc"
)

// Fanout mantiene los clientes gRPC a los peers.
type Fanout struct {
	self  string // ID propio
	peers map[string]pb.ReplicatorClient
}

// NewFanout crea conexiones salientes; si sólo hay un nodo devuelve nil.
func NewFanout(cfg *Config, selfID string) *Fanout {
	if len(cfg.Nodes) <= 1 {
		return nil
	}
	f := &Fanout{self: selfID, peers: make(map[string]pb.ReplicatorClient)}
	for _, n := range cfg.Nodes {
		if n.ID == selfID {
			continue
		}
		cc, err := grpc.Dial(n.Host, grpc.WithInsecure())
		if err != nil {
			log.Printf("[cluster] no conecta con %s: %v", n.ID, err)
			continue
		}
		f.peers[n.ID] = pb.NewReplicatorClient(cc)
	}
	return f
}

// Broadcast envía el lote de mensajes a todos los peers (fire-and-forget).
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
