package badgermeta

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/MateoRamirezRubio1/project_MOM/internal/ports/outbound"
	"github.com/dgraph-io/badger/v4"
)

const topicPrefix = "t:" // t:<topic> -> partitions(uint32)

// Catalog cumple el puerto outbound.MetaStore y persiste en BadgerDB.
type Catalog struct{ db *badger.DB }

func New(db *badger.DB) *Catalog { return &Catalog{db: db} }

// ------------------------------------------------------------------
// MetaStore implementation
// ------------------------------------------------------------------

func (c *Catalog) CreateTopic(_ context.Context, name string, partitions int, _ string) error {
	return c.db.Update(func(txn *badger.Txn) error {
		key := []byte(topicPrefix + name)
		if _, err := txn.Get(key); err == nil {
			return fmt.Errorf("topic %q already exists", name)
		} else if err != badger.ErrKeyNotFound {
			return err
		}
		return txn.Set(key, u32(partitions))
	})
}

func (c *Catalog) GetTopic(_ context.Context, name string) (int, error) {
	var p int
	err := c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(topicPrefix + name))
		if err != nil {
			return err
		}
		val, _ := item.ValueCopy(nil)
		p = int(b2u32(val))
		return nil
	})
	return p, err
}

func (c *Catalog) ListTopics(_ context.Context) ([]string, error) {
	var out []string
	err := c.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Prefix: []byte(topicPrefix)})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			name := strings.TrimPrefix(string(it.Item().Key()), topicPrefix)
			out = append(out, name)
		}
		return nil
	})
	return out, err
}

// Offsets delegation to Badger message store (no‑op here)
func (c *Catalog) GetOffset(context.Context, string, string, int) (uint64, error) {
	return 0, badger.ErrKeyNotFound
}
func (c *Catalog) CommitOffset(context.Context, string, string, int, uint64) error { return nil }

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------
func u32(i int) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(i))
	return b
}
func b2u32(b []byte) uint32 { return binary.BigEndian.Uint32(b) }

// Interface guard
var _ outbound.MetaStore = (*Catalog)(nil)
