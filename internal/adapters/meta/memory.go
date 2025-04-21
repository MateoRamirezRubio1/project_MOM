package meta

import (
	"context"
	"errors"
	"strconv"
	"sync"
)

var ErrExists = errors.New("topic exists")

type memoryCatalog struct {
	mu      sync.RWMutex
	topics  map[string]int               // topic -> partitions
	offsets map[string]map[string]uint64 // group -> topic:part -> offset
}

func NewMemoryCatalog() *memoryCatalog {
	return &memoryCatalog{
		topics:  make(map[string]int),
		offsets: make(map[string]map[string]uint64),
	}
}

// -------- tópicos --------

func (m *memoryCatalog) CreateTopic(_ context.Context, name string, p int, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.topics[name]; ok {
		return ErrExists
	}
	m.topics[name] = p
	return nil
}

func (m *memoryCatalog) GetTopic(_ context.Context, name string) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.topics[name]
	if !ok {
		return 0, errors.New("topic not found")
	}
	return p, nil
}

func (m *memoryCatalog) ListTopics(_ context.Context) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]string, 0, len(m.topics))
	for k := range m.topics {
		keys = append(keys, k)
	}
	return keys, nil
}

// -------- offsets (consumer groups) --------

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
