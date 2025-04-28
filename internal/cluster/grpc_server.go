package cluster

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	pb "github.com/MateoRamirezRubio1/project_MOM/internal/clusterpb"
	"github.com/MateoRamirezRubio1/project_MOM/internal/domain/model"
	"github.com/MateoRamirezRubio1/project_MOM/internal/ports/outbound"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

/*──────────  variables rellenadas por wiring.go ──────────*/

var GlobalCfg *Config
var GlobalSelfID string
var GlobalMeta outbound.MetaStore

/*──────────  servicio gRPC  ──────────*/

type replicaSrv struct {
	pb.UnimplementedReplicatorServer
	store outbound.MessageStore
}

// ---------- Replicate ----------

func (s *replicaSrv) Replicate(ctx context.Context,
	in *pb.ReplicateRequest,
) (*pb.ReplicateAck, error) {

	for _, m := range in.Batch {
		_, _ = s.store.Append(ctx, model.Message{
			ID:       uuid.MustParse(m.Uuid),
			Topic:    m.Topic,
			PartID:   int(m.Part),
			Offset:   m.Offset,
			Key:      m.Key,
			Producer: m.User,
			Payload:  m.Payload,
		})
		TrackNextOffset(m.Topic, int(m.Part), m.Offset+1)
	}
	return &pb.ReplicateAck{}, nil
}

/*──────────  ping  ──────────*/

func (s *replicaSrv) Ping(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

/*──────────  range para catch-up  ──────────*/

func (s *replicaSrv) GetRange(ctx context.Context,
	in *pb.RangeRequest,
) (*pb.RangeBatch, error) {

	max := int(in.To - in.From)
	if in.To == 0 {
		max = 1_000_000
	}
	msgs, err := s.store.Read(ctx, in.Topic, int(in.Part), in.From, max)
	if err != nil {
		return nil, err
	}
	out := make([]*pb.Message, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, &pb.Message{
			Uuid:    m.ID.String(),
			Topic:   m.Topic,
			Part:    uint32(m.PartID),
			Offset:  m.Offset,
			Key:     m.Key,
			User:    m.Producer,
			Payload: m.Payload,
		})
	}
	return &pb.RangeBatch{Batch: out}, nil
}

/*──────────  arranque y reconciliación  ──────────*/

func StartGRPCServer(addr string, store outbound.MessageStore, meta outbound.MetaStore) {
	// guarda la referencia a MetaStore para el reconciliador
	GlobalMeta = meta

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("[cluster] listen %s: %v", addr, err)
	}
	s := grpc.NewServer()
	pb.RegisterReplicatorServer(s, &replicaSrv{store: store})
	log.Printf("[cluster] gRPC en %s", addr)

	/*── reconciliación cada 30 s ─*/
	go func() {
		t := time.NewTicker(30 * time.Second)
		for range t.C {
			reconcile(store)
		}
	}()

	go func() { _ = s.Serve(lis) }()
}

/*──────────  reconciliador  ──────────*/

// pide a UN peer vivo todos los mensajes que le falten en cada (topic,part)
func reconcile(store outbound.MessageStore) {
	if GlobalMeta == nil {
		return
	}
	ctx := context.Background()

	// 1) snapshot de los HWM en RAM
	local := Snapshot()

	// 2) prepara fan-out
	f := NewFanout(GlobalCfg, GlobalSelfID)
	if f == nil {
		return
	}

	// 3) por cada topic y part que existan en el catálogo
	topics, _ := GlobalMeta.ListTopics(ctx)
	for _, tp := range topics {
		parts, err := GlobalMeta.GetTopic(ctx, tp) // nº de particiones
		if err != nil {
			continue
		}
		for p := 0; p < parts; p++ {
			key := fmt.Sprintf("%s:%d", tp, p)
			from := local[key] // 0 si no existe
			// busca el primer peer vivo
			for peer := range f.peers {
				_ = f.CatchUp(ctx, peer, store, tp, p, from, 0)
				break
			}
		}
	}
}

func atoi(s string) int { i, _ := strconv.Atoi(s); return i }
