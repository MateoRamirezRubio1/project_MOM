package outbound

import "context"

type AuthStore interface {
	Validate(ctx context.Context, token string) (username string, ok bool)
}
