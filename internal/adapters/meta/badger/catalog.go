package badgermeta

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/MateoRamirezRubio1/project_MOM/internal/ports/outbound"
	"github.com/dgraph-io/badger/v4"
)

const (
	topicPrefix   = "t:" // t:<topic>           -> partitions(uint32)
	creatorPrefix = "c:" // c:<kind>:<name>     -> json {creator}
	offsetPrefix  = "o:" // o:<group>:<topic>:<part> -> offset(uint64)

	queuePrefix = "q:" // q:<queue> (valor vacío)
)

type creatorRec struct {
	User string `json:"user"`
}

// Catalog implementa MetaStore sobre BadgerDB.
type Catalog struct{ db *badger.DB }

func New(db *badger.DB) *Catalog { return &Catalog{db: db} }

var _ outbound.MetaStore = (*Catalog)(nil)

// ------------------------------------------------------------------
// TOPICS
// ------------------------------------------------------------------

func (c *Catalog) CreateTopic(_ context.Context, name string, parts int, user string) error {
	return c.db.Update(func(txn *badger.Txn) error {
		k := []byte(topicPrefix + name)
		if _, err := txn.Get(k); err == nil {
			return fmt.Errorf("topic %q already exists", name)
		} else if err != badger.ErrKeyNotFound {
			return err
		}
		if err := txn.Set(k, u32(parts)); err != nil {
			return err
		}
		meta, _ := json.Marshal(creatorRec{User: user})
		return txn.Set([]byte(creatorPrefix+"topic:"+name), meta)
	})
}

func (c *Catalog) GetTopic(_ context.Context, name string) (int, error) {
	var p int
	err := c.db.View(func(txn *badger.Txn) error {
		it, err := txn.Get([]byte(topicPrefix + name))
		if err != nil {
			return err
		}
		val, _ := it.ValueCopy(nil)
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

func (c *Catalog) DeleteTopic(_ context.Context, name, user string) error {
	return c.db.Update(func(txn *badger.Txn) error {
		ck := []byte(creatorPrefix + "topic:" + name)
		item, err := txn.Get(ck)
		if err != nil {
			return fmt.Errorf("topic not found")
		}
		var rec creatorRec
		val, _ := item.ValueCopy(nil)
		_ = json.Unmarshal(val, &rec)
		if rec.User != user {
			return fmt.Errorf("only creator can delete")
		}
		if err := txn.Delete([]byte(topicPrefix + name)); err != nil {
			return err
		}
		return txn.Delete(ck)
	})
}

// ------------------------------------------------------------------
// QUEUES
// ------------------------------------------------------------------

func (c *Catalog) CreateQueue(_ context.Context, name, user string) error {
	return c.db.Update(func(txn *badger.Txn) error {
		k := []byte(queuePrefix + name)
		if _, err := txn.Get(k); err == nil {
			return fmt.Errorf("queue exists")
		} else if err != badger.ErrKeyNotFound {
			return err
		}
		if err := txn.Set(k, []byte{}); err != nil {
			return err
		}
		meta, _ := json.Marshal(creatorRec{User: user})
		return txn.Set([]byte(creatorPrefix+"queue:"+name), meta)
	})
}

func (c *Catalog) ListQueues(_ context.Context) ([]string, error) {
	var out []string
	err := c.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Prefix: []byte(queuePrefix)})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			out = append(out, strings.TrimPrefix(string(it.Item().Key()), queuePrefix))
		}
		return nil
	})
	return out, err
}

func (c *Catalog) DeleteQueue(_ context.Context, name, user string) error {
	return c.db.Update(func(txn *badger.Txn) error {
		ck := []byte(creatorPrefix + "queue:" + name)
		item, err := txn.Get(ck)
		if err != nil {
			return fmt.Errorf("queue not found")
		}
		var rec creatorRec
		val, _ := item.ValueCopy(nil)
		_ = json.Unmarshal(val, &rec)
		if rec.User != user {
			return fmt.Errorf("only creator can delete")
		}
		if err := txn.Delete([]byte(queuePrefix + name)); err != nil {
			return err
		}
		return txn.Delete(ck)
	})
}

// ------------------------------------------------------------------
// OFFSETS (consumer groups)
// ------------------------------------------------------------------

func (c *Catalog) GetOffset(_ context.Context, grp, topic string, part int) (uint64, error) {
	k := offsetPrefix + grp + ":" + topic + ":" + strconv.Itoa(part)
	var off uint64
	err := c.db.View(func(txn *badger.Txn) error {
		it, err := txn.Get([]byte(k))
		if err != nil {
			return err
		}
		val, _ := it.ValueCopy(nil)
		off = b2u64(val)
		return nil
	})
	return off, err
}

func (c *Catalog) CommitOffset(_ context.Context, grp, topic string, part int, off uint64) error {
	return c.db.Update(func(txn *badger.Txn) error {
		k := offsetPrefix + grp + ":" + topic + ":" + strconv.Itoa(part)
		return txn.Set([]byte(k), u64(off))
	})
}

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

func u32(i int) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(i))
	return b
}
func b2u32(b []byte) uint32 { return binary.BigEndian.Uint32(b) }

func u64(i uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, i)
	return b
}
func b2u64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }
