package inbound

import (
	"context"

	"github.com/MateoRamirezRubio1/project_MOM/internal/domain/model"
)

type Consumer interface {
	// Pull mensajes de una partición (modo tópicos)
	Pull(ctx context.Context, topic, group string, part int, max int) ([]model.Message, error)
	// Commit offset leído
	Commit(ctx context.Context, topic, group string, part int, offset uint64) error
}
