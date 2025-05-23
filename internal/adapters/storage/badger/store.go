package badgerstore

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/MateoRamirezRubio1/project_MOM/internal/cluster"
	"github.com/MateoRamirezRubio1/project_MOM/internal/domain/model"
	"github.com/MateoRamirezRubio1/project_MOM/internal/ports/outbound"
	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
)

const (
	msgPrefix    = "m:" // m:<topic>:<part>:<offset>
	hwmPrefix    = "h:" // h:<topic>:<part>
	qPrefix      = "q:" // q:<queue>:<seq>
	infPrefix    = "f:" // f:<queue>:<uuid>
	offsetPrefix = "o:" // o:<group>:<topic>:<part> -> offset(uint64)
)

// ------------------------------------------------------------------
// Store
// ------------------------------------------------------------------

type Store struct{ db *badger.DB }

func New(dir string) (*Store, error) {
	opts := badger.DefaultOptions(filepath.Clean(dir)).WithLoggingLevel(badger.ERROR)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// Exponer la instancia para el MetaStore
func (s *Store) DB() *badger.DB { return s.db }

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

func join(parts ...string) string { return strings.Join(parts, ":") }
func key(parts ...string) []byte  { return []byte(join(parts...)) }

func u64(i uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, i)
	return b
}
func b2u64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }

// ------------------------------------------------------------------
// Tópicos
// ------------------------------------------------------------------

// Append agrega un mensaje NUEVO generado por este nodo.
func (s *Store) Append(_ context.Context, msg model.Message) (uint64, error) {
	var offset uint64
	err := s.db.Update(func(txn *badger.Txn) error {
		hwmKey := key(hwmPrefix, msg.Topic, strconv.Itoa(msg.PartID))
		item, err := txn.Get(hwmKey)
		if err == badger.ErrKeyNotFound {
			offset = 0
		} else if err == nil {
			val, _ := item.ValueCopy(nil)
			offset = b2u64(val)
		} else {
			return err
		}

		/* si viene del publicador ya trae UUID,
		   si viene del reconciliador es uuid.Nil y le ponemos uno */
		if msg.ID == uuid.Nil {
			msg.ID = uuid.New()
		}
		msg.Offset = offset

		js, _ := json.Marshal(msg)
		if err := txn.Set(
			key(msgPrefix, msg.Topic, strconv.Itoa(msg.PartID), strconv.FormatUint(offset, 10)), js); err != nil {
			return err
		}
		return txn.Set(hwmKey, u64(offset+1))
	})
	if err == nil {
		// refresca hwm en memoria para reconciliación
		cluster.TrackNextOffset(msg.Topic, msg.PartID, offset+1)
	}
	return offset, err
}

// AppendWithOffset inserta un mensaje VENIDO DE OTRO NODO conservando offset.
func (s *Store) AppendWithOffset(_ context.Context, msg model.Message) error {
	return s.db.Update(func(txn *badger.Txn) error {
		partStr := strconv.Itoa(msg.PartID)
		hwmKey := key(hwmPrefix, msg.Topic, partStr)
		msgKey := key(msgPrefix, msg.Topic, partStr, strconv.FormatUint(msg.Offset, 10))

		// 1) ¿ya lo tenía?
		if _, err := txn.Get(msgKey); err == nil {
			// Aun así se puede necesitar subir el HWM si se está rezagado
			curNext := uint64(0)
			if item, err := txn.Get(hwmKey); err == nil {
				val, _ := item.ValueCopy(nil)
				curNext = b2u64(val)
			}
			if next := msg.Offset + 1; next > curNext {
				_ = txn.Set(hwmKey, u64(next))
				cluster.TrackNextOffset(msg.Topic, msg.PartID, next) // RAM
			}
			return nil
		}

		// 2) insertar el mensaje
		if msg.ID == uuid.Nil {
			msg.ID = uuid.New()
		}
		js, _ := json.Marshal(msg)
		if err := txn.Set(msgKey, js); err != nil {
			return err
		}

		// 3) dejar el HWM persistido al día
		next := msg.Offset + 1
		_ = txn.Set(hwmKey, u64(next))
		cluster.TrackNextOffset(msg.Topic, msg.PartID, next) // RAM
		return nil
	})
}

func (s *Store) Read(_ context.Context, topic string, part int, from uint64, max int) ([]model.Message, error) {
	prefix := key(msgPrefix, topic, strconv.Itoa(part))
	start := key(msgPrefix, topic, strconv.Itoa(part), strconv.FormatUint(from, 10))
	out := make([]model.Message, 0, max)

	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Prefix: prefix})
		defer it.Close()
		for it.Seek(start); it.Valid(); it.Next() {
			if len(out) >= max {
				break
			}
			var m model.Message
			val, _ := it.Item().ValueCopy(nil)
			if err := json.Unmarshal(val, &m); err != nil {
				return err
			}
			out = append(out, m)
		}
		return nil
	})
	return out, err
}

func (s *Store) Delete(context.Context, string, int, uint64) error { return nil }

// ------------------------------------------------------------------
// Colas
// ------------------------------------------------------------------

func (s *Store) CreateQueue(context.Context, string) error { return nil }

func (s *Store) Enqueue(_ context.Context, q string, msg model.Message) error {
	if msg.ID == uuid.Nil {
		msg.ID = uuid.New()
	}
	js, _ := json.Marshal(msg)
	seq := uuid.New().String()
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key(qPrefix, q, seq), js)
	})
}

func (s *Store) Dequeue(_ context.Context, q string) (*model.Message, error) {
	var res *model.Message
	var firstKey []byte

	err := s.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Prefix: key(qPrefix, q)})
		defer it.Close()
		it.Rewind()
		if !it.Valid() {
			return nil // empty queue
		}
		firstKey = it.Item().KeyCopy(nil)
		val, _ := it.Item().ValueCopy(nil)
		var m model.Message
		if err := json.Unmarshal(val, &m); err != nil {
			return err
		}
		res = &m

		// move to in‑flight
		if err := txn.Delete(firstKey); err != nil {
			return err
		}
		exp := u64(uint64(time.Now().Add(30 * time.Second).Unix()))
		return txn.Set(key(infPrefix, q, m.ID.String()), exp)
	})
	return res, err
}

func (s *Store) Ack(_ context.Context, q string, id uuid.UUID) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key(infPrefix, q, id.String()))
	})
}

// ------------------------------------------------------------------
// Consumer Groups & Offsets
// ------------------------------------------------------------------

// CommitOffset guarda el offset para un grupo de consumidores.
func (s *Store) CommitOffset(ctx context.Context, group, topic string, part int, offset uint64) error {
	offsetKey := key(offsetPrefix, group, topic, strconv.Itoa(part))
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(offsetKey, u64(offset))
	})
}

// ------------------------------------------------------------------
// Re‑enqueue loop (handles TTL)
// ------------------------------------------------------------------

func (s *Store) StartRequeueLoop() {
	go func() {
		tick := time.NewTicker(5 * time.Second)
		for range tick.C {
			now := uint64(time.Now().Unix())
			_ = s.db.Update(func(txn *badger.Txn) error {
				it := txn.NewIterator(badger.IteratorOptions{Prefix: []byte(infPrefix)})
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					k := string(it.Item().Key())
					expBytes, _ := it.Item().ValueCopy(nil)
					exp := b2u64(expBytes)
					if now <= exp {
						continue
					}
					parts := strings.Split(k, ":")
					if len(parts) != 3 {
						_ = txn.Delete(it.Item().Key()) // corrupted
						continue
					}
					q, id := parts[1], parts[2]
					// reinserta stub (en real se movería payload original)
					stub := model.Message{ID: uuid.MustParse(id), Payload: []byte("expired")}
					js, _ := json.Marshal(stub)
					seq := uuid.New().String()
					if err := txn.Set(key(qPrefix, q, seq), js); err != nil {
						return err
					}
					_ = txn.Delete(it.Item().Key())
				}
				return nil
			})
		}
	}()
}

// ------------------------------------------------------------------
// Interface guard
// ------------------------------------------------------------------

var _ outbound.MessageStore = (*Store)(nil)
