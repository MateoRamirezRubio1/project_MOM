package storage

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MateoRamirezRubio1/project_MOM/internal/domain/model"
	"github.com/google/uuid"
)

// ---------- helper structures -----------------------------------

type partLog struct {
	mu     sync.Mutex
	events []model.Message
	next   uint64
}

type queue struct {
	mu       sync.Mutex
	items    []model.Message
	inFlight map[uuid.UUID]time.Time
}

// ---------- memoryStore -----------------------------------------

type memoryStore struct {
	mu     sync.RWMutex
	parts  map[string]*partLog // topic:part -> log
	queues map[string]*queue   // queue name -> queue struct
}

func NewMemoryStore() *memoryStore {
	return &memoryStore{
		parts:  make(map[string]*partLog),
		queues: make(map[string]*queue),
	}
}

// ---------- TÓPICOS (append / read) ------------------------------

func (m *memoryStore) Append(_ context.Context, msg model.Message) (uint64, error) {
	key := msg.Topic + ":" + strconv.Itoa(msg.PartID)

	m.mu.RLock()
	pl, ok := m.parts[key]
	m.mu.RUnlock()
	if !ok {
		m.mu.Lock()
		pl = &partLog{}
		m.parts[key] = pl
		m.mu.Unlock()
	}

	pl.mu.Lock()
	defer pl.mu.Unlock()
	msg.ID = uuid.New()
	msg.Offset = atomic.AddUint64(&pl.next, 1) - 1
	pl.events = append(pl.events, msg)
	return msg.Offset, nil
}

func (m *memoryStore) Read(_ context.Context, topic string, part int, from uint64, max int) ([]model.Message, error) {
	key := topic + ":" + strconv.Itoa(part)
	m.mu.RLock()
	pl, ok := m.parts[key]
	m.mu.RUnlock()
	if !ok {
		return nil, nil
	}
	pl.mu.Lock()
	defer pl.mu.Unlock()
	if from >= pl.next {
		return nil, nil
	}
	end := from + uint64(max)
	if end > pl.next {
		end = pl.next
	}
	return append([]model.Message(nil), pl.events[from:end]...), nil
}

func (m *memoryStore) Delete(_ context.Context, _ string, _ int, _ uint64) error { return nil }

// ---------- COLAS ------------------------------------------------

func (m *memoryStore) queue(q string) *queue {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.queues[q]; !ok {
		m.queues[q] = &queue{inFlight: make(map[uuid.UUID]time.Time)}
		// rutina de requeue
		go m.requeueLoop(q, m.queues[q])
	}
	return m.queues[q]
}

func (m *memoryStore) Enqueue(_ context.Context, q string, msg model.Message) error {
	if msg.ID == uuid.Nil {
		msg.ID = uuid.New()
	}
	qu := m.queue(q)
	qu.mu.Lock()
	defer qu.mu.Unlock()
	qu.items = append(qu.items, msg)
	return nil
}

func (m *memoryStore) Dequeue(_ context.Context, q string) (*model.Message, error) {
	qu, ok := m.queues[q]
	if !ok {
		return nil, errors.New("queue not found")
	}
	qu.mu.Lock()
	defer qu.mu.Unlock()
	if len(qu.items) == 0 {
		return nil, nil
	}
	msg := qu.items[0]
	qu.items = qu.items[1:]
	qu.inFlight[msg.ID] = time.Now().Add(30 * time.Second)
	return &msg, nil
}

func (m *memoryStore) Ack(_ context.Context, q string, id uuid.UUID) error {
	qu, ok := m.queues[q]
	if !ok {
		return errors.New("queue not found")
	}
	qu.mu.Lock()
	defer qu.mu.Unlock()
	delete(qu.inFlight, id)
	return nil
}

// -- background goroutine re‑enqueues expired in‑flight messages ---
func (m *memoryStore) requeueLoop(name string, qu *queue) {
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		qu.mu.Lock()
		now := time.Now()
		for id, exp := range qu.inFlight {
			if now.After(exp) {
				// reinserta al final
				qu.items = append(qu.items, model.Message{ID: id, Payload: []byte("expired")})
				delete(qu.inFlight, id)
			}
		}
		qu.mu.Unlock()
	}
}
