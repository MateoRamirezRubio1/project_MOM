package auth

import (
	"context"
	"sync"
)

type memoryAuth struct {
	mu    sync.RWMutex
	store map[string]string // token -> username
}

func NewInMemory() *memoryAuth {
	m := &memoryAuth{store: make(map[string]string)}
	// usuario por defecto
	m.store["alice"] = "alice"
	return m
}

func (a *memoryAuth) Validate(_ context.Context, token string) (string, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	u, ok := a.store[token]
	return u, ok
}
