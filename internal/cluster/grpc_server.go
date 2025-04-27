package cluster

import (
	"context"
	"log"
	"net"

	"google.golang.org/grpc"

	pb "github.com/MateoRamirezRubio1/project_MOM/internal/clusterpb"
	"github.com/MateoRamirezRubio1/project_MOM/internal/domain/model"
	"github.com/MateoRamirezRubio1/project_MOM/internal/ports/outbound"
)

// ------------------------------------------------------------------
// gRPC service -   recibe lotes replicados desde los peers
// ------------------------------------------------------------------

type replicaSrv struct {
	pb.UnimplementedReplicatorServer
	store outbound.MessageStore
}

func (s *replicaSrv) Replicate(
	ctx context.Context,
	in *pb.ReplicateRequest,
) (*pb.ReplicateAck, error) {

	for _, m := range in.Batch {
		// Sólo nos interesa insertar la copia local; ignoramos offset devuelto.
		_, _ = s.store.Append(ctx, model.Message{
			Topic:   m.Topic,
			PartID:  int(m.Part),
			Payload: m.Json, // el productor remoto ya serializó el payload
		})
	}
	return &pb.ReplicateAck{}, nil
}

// ------------------------------------------------------------------
// Bootstrap del servidor gRPC
// ------------------------------------------------------------------

func StartGRPCServer(addr string, store outbound.MessageStore) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("[cluster] listen %s: %v", addr, err)
	}
	s := grpc.NewServer()
	pb.RegisterReplicatorServer(s, &replicaSrv{store: store})
	log.Printf("[cluster] gRPC escuchando en %s", addr)

	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("[cluster] Serve: %v", err)
		}
	}()
}
