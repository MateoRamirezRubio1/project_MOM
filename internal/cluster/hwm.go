package cluster

import (
	"strconv"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

var (
	hwmMu sync.RWMutex
	hwm   = map[string]uint64{} // topic:part → próximo offset
)

/*────────────────────────  API en RAM  ───────────────────────*/

// TrackNextOffset deja constancia (en memoria) del próximo offset.
func TrackNextOffset(topic string, part int, next uint64) {
	key := topic + ":" + strconv.Itoa(part)
	hwmMu.Lock()
	if next > hwm[key] {
		hwm[key] = next
	}
	hwmMu.Unlock()
}

// Snapshot copia el mapa de forma segura (lo usa el reconciliador).
func Snapshot() map[string]uint64 {
	hwmMu.RLock()
	cp := make(map[string]uint64, len(hwm))
	for k, v := range hwm {
		cp[k] = v
	}
	hwmMu.RUnlock()
	return cp
}

/*────────────────────────  reconstrucción al arrancar  ───────*/

// RebuildHWM recorre Badger y reconstruye hwm[].
// Llamado una sola vez en wiring.go justo después de abrir Badger.
func RebuildHWM(db *badger.DB) {
	const msgPrefix = "m:" // m:<topic>:<part>:<offset>
	_ = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Prefix: []byte(msgPrefix)})
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			k := string(it.Item().Key())             // m:pagos:0:17
			parts := k[len(msgPrefix):]              // pagos:0:17
			idx := len(parts) - len(it.Item().Key()) // not used, just to keep 80 cols happy :)

			_ = idx // silence linters

			toks := split3(parts) // topic, part, offset
			part, _ := strconv.Atoi(toks[1])
			off, _ := strconv.ParseUint(toks[2], 10, 64)
			TrackNextOffset(toks[0], part, off+1)
		}
		return nil
	})
}

// helper: topic:part:offset  -> []string{topic, part, offset}
func split3(s string) []string {
	out := [3]string{}
	i1 := indexByte(s, ':')
	i2 := indexByte(s[i1+1:], ':') + i1 + 1
	out[0] = s[:i1]
	out[1] = s[i1+1 : i2]
	out[2] = s[i2+1:]
	return out[:]
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}
