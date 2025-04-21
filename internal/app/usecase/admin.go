package usecase

import (
	"context"

	"github.com/MateoRamirezRubio1/project_MOM/internal/ports/inbound"
	"github.com/MateoRamirezRubio1/project_MOM/internal/ports/outbound"
)

type adminUC struct {
	meta outbound.MetaStore
	auth outbound.AuthStore
}

func NewAdmin(meta outbound.MetaStore, auth outbound.AuthStore) inbound.Admin {
	return &adminUC{meta: meta, auth: auth}
}

func (a *adminUC) CreateTopic(ctx context.Context, name string, partitions int, user string) error {
	return a.meta.CreateTopic(ctx, name, partitions, user)
}

func (a *adminUC) ListTopics(ctx context.Context) ([]string, error) {
	return a.meta.ListTopics(ctx)
}
