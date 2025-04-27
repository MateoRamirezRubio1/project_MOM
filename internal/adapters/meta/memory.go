package meta

import (
	"context"
	"errors"
	"strconv"
	"sync"
)

var (
	ErrExists     = errors.New("already exists")
	ErrNotFound   = errors.New("not found")
	ErrNotCreator = errors.New("only creator can delete")
)

type memoryCatalog struct {
	mu sync.RWMutex

	// topic -> {partitions, creator}
	topics map[string]struct {
		parts   int
		creator string
	}

	// queue -> creator
	queues map[string]string

	// group -> topic:part -> offset
	offsets map[string]map[string]uint64
}

func NewMemoryCatalog() *memoryCatalog {
	return &memoryCatalog{
		topics: make(map[string]struct {
			parts   int
			creator string
		}),
		queues:  make(map[string]string),
		offsets: make(map[string]map[string]uint64),
	}
}

// -------- TOPICS --------
func (m *memoryCatalog) CreateTopic(_ context.Context, name string, p int, user string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.topics[name]; ok {
		return ErrExists
	}
	m.topics[name] = struct {
		parts   int
		creator string
	}{p, user}
	return nil
}

func (m *memoryCatalog) GetTopic(_ context.Context, name string) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.topics[name]
	if !ok {
		return 0, ErrNotFound
	}
	return t.parts, nil
}

func (m *memoryCatalog) ListTopics(_ context.Context) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.topics))
	for k := range m.topics {
		out = append(out, k)
	}
	return out, nil
}

func (m *memoryCatalog) DeleteTopic(_ context.Context, name, user string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.topics[name]
	if !ok {
		return ErrNotFound
	}
	if t.creator != user {
		return ErrNotCreator
	}
	delete(m.topics, name)
	return nil
}

// -------- QUEUES --------
func (m *memoryCatalog) CreateQueue(_ context.Context, name, user string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.queues[name]; ok {
		return ErrExists
	}
	m.queues[name] = user
	return nil
}

func (m *memoryCatalog) ListQueues(_ context.Context) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.queues))
	for q := range m.queues {
		out = append(out, q)
	}
	return out, nil
}

func (m *memoryCatalog) DeleteQueue(_ context.Context, name, user string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	creator, ok := m.queues[name]
	if !ok {
		return ErrNotFound
	}
	if creator != user {
		return ErrNotCreator
	}
	delete(m.queues, name)
	return nil
}

// -------- OFFSETS (consumer groups) --------
func key(topic string, part int) string { return topic + ":" + strconv.Itoa(part) }

func (m *memoryCatalog) GetOffset(_ context.Context, grp, topic string, part int) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.offsets[grp] == nil {
		return 0, nil
	}
	return m.offsets[grp][key(topic, part)], nil
}

func (m *memoryCatalog) CommitOffset(_ context.Context, grp, topic string, part int, off uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.offsets[grp] == nil {
		m.offsets[grp] = make(map[string]uint64)
	}
	m.offsets[grp][key(topic, part)] = off
	return nil
}
