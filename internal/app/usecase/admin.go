package usecase

import (
	"context"

	"github.com/MateoRamirezRubio1/project_MOM/internal/ports/inbound"
	"github.com/MateoRamirezRubio1/project_MOM/internal/ports/outbound"
)

type adminUC struct{ meta outbound.MetaStore }

func NewAdmin(meta outbound.MetaStore) inbound.Admin { return &adminUC{meta: meta} }

// TÓPICOS
func (a *adminUC) CreateTopic(ctx context.Context, n string, p int, u string) error {
	return a.meta.CreateTopic(ctx, n, p, u)
}
func (a *adminUC) ListTopics(ctx context.Context) ([]string, error) {
	return a.meta.ListTopics(ctx)
}
func (a *adminUC) DeleteTopic(ctx context.Context, n, u string) error {
	return a.meta.DeleteTopic(ctx, n, u)
}

// COLAS
func (a *adminUC) CreateQueue(ctx context.Context, n, u string) error {
	return a.meta.CreateQueue(ctx, n, u)
}
func (a *adminUC) ListQueues(ctx context.Context) ([]string, error) {
	return a.meta.ListQueues(ctx)
}
func (a *adminUC) DeleteQueue(ctx context.Context, n, u string) error {
	return a.meta.DeleteQueue(ctx, n, u)
}
