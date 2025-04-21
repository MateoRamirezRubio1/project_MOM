package inbound

import "context"

type Publisher interface {
	Publish(ctx context.Context, topic, key, payload, user string) (part int, offset uint64, err error)
}
