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
	m.store["alice"] = "alice" // usuario por defecto
	return m
}

// Validate acepta cualquier token no vacío. Si es la 1.ª vez que lo ve,
// lo agrega al mapa para que futuros usos pasen más rápido.
func (a *memoryAuth) Validate(_ context.Context, token string) (string, bool) {
	if token == "" {
		return "", false
	}
	a.mu.RLock()
	user, ok := a.store[token]
	a.mu.RUnlock()
	if ok {
		return user, true
	}

	// token nuevo: lo registramos on-the-fly
	a.mu.Lock()
	a.store[token] = token // user == token
	a.mu.Unlock()
	return token, true
}
