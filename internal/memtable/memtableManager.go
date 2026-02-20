package memtable

import (
	"fmt"
	"sort"
	"time"

	"kv-engine/internal/model"
)

type Factory func() Memtable

// Drzi N memtable instanci: 1 RW (active) + do N-1 RO u roQueue (FIFO).
// Flush je potreban kada popunimo svih N tabela (nema slobodnog slota).
type MemtableManager struct {
	tables       []Memtable
	used         []bool
	activeFrozen bool

	active  int   // index RW memtable-a
	roQueue []int // FIFO slotova koji su RO (najstariji prvi)

	factory Factory
}

func NewMemtableManager(n int, factory Factory) (MemtableManagerIface, error) {
	if n <= 0 {
		return nil, fmt.Errorf("memtable instances N must be > 0")
	}
	if factory == nil {
		return nil, fmt.Errorf("memtable factory is nil")
	}

	m := &MemtableManager{
		tables:  make([]Memtable, n),
		used:    make([]bool, n),
		active:  0,
		roQueue: make([]int, 0, max(0, n-1)),
		factory: factory,
	}

	// inicijalno imamo jednu RW tabelu
	m.tables[0] = factory()
	if m.tables[0] == nil {
		return nil, fmt.Errorf("factory returned nil memtable")
	}
	m.used[0] = true

	return m, nil
}

// Get: prvo active, pa RO od najnovijeg ka najstarijem (da uvek vratis najnoviju verziju kljuca).
func (m *MemtableManager) Get(key string) model.GetResult {
	// 1) active (najnovije)
	if res := m.tables[m.active].Get(key); res.Found {
		return res
	}

	// 2) RO tabele od najnovije ka najstarijoj
	for i := len(m.roQueue) - 1; i >= 0; i-- {
		idx := m.roQueue[i]
		if res := m.tables[idx].Get(key); res.Found {
			return res
		}
	}

	return model.GetResult{Found: false}
}

func (m *MemtableManager) GetMergeOperands(structure model.StructureType, key string) []model.Record {
	out := make([]model.Record, 0)

	// active first (najnoviji), zatim RO od najnovijeg ka najstarijem
	out = append(out, m.tables[m.active].GetMergeOperands(structure, key)...)
	for i := len(m.roQueue) - 1; i >= 0; i-- {
		idx := m.roQueue[i]
		out = append(out, m.tables[idx].GetMergeOperands(structure, key)...)
	}
	return out
}

func (m *MemtableManager) ListLiveKeysInRange(startKey, endKey string) ([]string, error) {
	if startKey == "" || endKey == "" {
		return nil, fmt.Errorf("range keys must not be empty")
	}
	if startKey > endKey {
		return nil, fmt.Errorf("invalid range: start key must be <= end key")
	}

	type keyState struct {
		seq       uint64
		tombstone bool
		expired   bool
	}

	now := uint64(time.Now().Unix())
	stateByKey := make(map[string]keyState)

	process := func(recs []model.Record) {
		for _, rec := range recs {
			if rec.Kind != model.RecordKindKV {
				continue
			}
			if rec.Key < startKey || rec.Key > endKey {
				continue
			}
			cur, ok := stateByKey[rec.Key]
			if ok && cur.seq >= rec.Seq {
				continue
			}
			stateByKey[rec.Key] = keyState{
				seq:       rec.Seq,
				tombstone: rec.Tombstone,
				expired:   rec.ExpiresAt > 0 && rec.ExpiresAt <= now,
			}
		}
	}

	if m.used[m.active] && m.tables[m.active] != nil {
		process(m.tables[m.active].RecordsSorted())
	}
	for i := len(m.roQueue) - 1; i >= 0; i-- {
		idx := m.roQueue[i]
		if !m.used[idx] || m.tables[idx] == nil {
			continue
		}
		process(m.tables[idx].RecordsSorted())
	}

	keys := make([]string, 0, len(stateByKey))
	for key, st := range stateByKey {
		if st.tombstone || st.expired {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

func (m *MemtableManager) ApplyBatchAtomically(records []model.Record) error {
	if len(records) == 0 {
		return nil
	}

	clone, err := m.deepClone()
	if err != nil {
		return err
	}

	for _, rec := range records {
		var (
			flushNeeded bool
			opErr       error
		)
		if rec.Kind == model.RecordKindKV && rec.Tombstone {
			flushNeeded, opErr = clone.Delete(rec)
		} else {
			flushNeeded, opErr = clone.Put(rec)
		}
		if opErr != nil {
			return opErr
		}
		if flushNeeded {
			return fmt.Errorf("batch exceeds memtable transactional capacity")
		}
	}

	*m = *clone
	return nil
}

func (m *MemtableManager) CanApplyBatchAtomically(records []model.Record) error {
	if len(records) == 0 {
		return nil
	}
	clone, err := m.deepClone()
	if err != nil {
		return err
	}
	for _, rec := range records {
		var (
			flushNeeded bool
			opErr       error
		)
		if rec.Kind == model.RecordKindKV && rec.Tombstone {
			flushNeeded, opErr = clone.Delete(rec)
		} else {
			flushNeeded, opErr = clone.Put(rec)
		}
		if opErr != nil {
			return opErr
		}
		if flushNeeded {
			return fmt.Errorf("batch exceeds memtable transactional capacity")
		}
	}
	return nil
}

// Put/Delete vracaju flushNeeded=true kad je active postala puna i nema slobodnog slota (tj. popunili smo svih N).
func (m *MemtableManager) Put(r model.Record) (bool, error) {
	m.tables[m.active].Put(r)
	return m.rotateIfNeeded()
}

func (m *MemtableManager) Delete(r model.Record) (bool, error) {
	m.tables[m.active].Delete(r)
	return m.rotateIfNeeded()
}

// rotateIfNeeded:
// - ako active nije puna -> (false,nil)
// - ako jeste -> prebaci active u RO queue
//   - ako ima slobodan slot -> napravi novi RW u tom slotu, vrati (false,nil)
//   - ako nema slobodnog slota -> vrati (true,nil) => flush needed
func (m *MemtableManager) rotateIfNeeded() (bool, error) {
	if !m.tables[m.active].IsFull() {
		return false, nil
	}

	// freeze active -> RO
	if !m.activeFrozen {
		m.roQueue = append(m.roQueue, m.active)
		m.activeFrozen = true
	}

	// nadji slobodan slot za novi RW
	free := m.findFreeSlot()
	if free == -1 {
		// svih N su zauzete => flush needed
		return true, nil
	}

	// kreiraj novi RW
	m.tables[free] = m.factory()
	if m.tables[free] == nil {
		return false, fmt.Errorf("factory returned nil memtable")
	}
	m.used[free] = true
	m.activeFrozen = false
	m.active = free

	return false, nil
}

func (m *MemtableManager) findFreeSlot() int {
	for i := 0; i < len(m.tables); i++ {
		if !m.used[i] {
			return i
		}
	}
	return -1
}

func (m *MemtableManager) NextFlushBatch() ([]model.Record, bool) {
	if len(m.roQueue) == 0 {
		return nil, false
	}

	idx := m.roQueue[0]
	copy(m.roQueue[0:], m.roQueue[1:])
	m.roQueue = m.roQueue[:len(m.roQueue)-1]

	recs := m.tables[idx].DrainSorted()

	// oslobodi slot
	m.tables[idx] = nil
	m.used[idx] = false

	// ako trenutno nemamo RW (active pokazuje na nil ili used=false),
	// napravi novi RW bas u ovom slotu
	if m.tables[m.active] == nil || !m.used[m.active] || m.activeFrozen {
		m.tables[idx] = m.factory()
		m.used[idx] = true
		m.active = idx
		m.activeFrozen = false
	}

	return recs, true
}

func (m *MemtableManager) ForceFlushAll() ([][]model.Record, error) {
	batches := make([][]model.Record, 0)

	if m.used[m.active] && m.tables[m.active] != nil && !m.activeFrozen {
		if len(m.tables[m.active].RecordsSorted()) > 0 {
			m.roQueue = append(m.roQueue, m.active)
			m.activeFrozen = true
		}
	}

	for {
		recs, ok := m.NextFlushBatch()
		if !ok {
			break
		}
		if len(recs) == 0 {
			continue
		}
		batches = append(batches, recs)
	}

	return batches, nil
}

func (m *MemtableManager) deepClone() (*MemtableManager, error) {
	clone := &MemtableManager{
		tables:       make([]Memtable, len(m.tables)),
		used:         make([]bool, len(m.used)),
		activeFrozen: m.activeFrozen,
		active:       m.active,
		roQueue:      append([]int(nil), m.roQueue...),
		factory:      m.factory,
	}
	copy(clone.used, m.used)

	for i := range m.tables {
		if !m.used[i] {
			continue
		}
		if m.tables[i] == nil {
			return nil, fmt.Errorf("memtable slot %d is marked used but table is nil", i)
		}
		clone.tables[i] = m.tables[i].Clone()
		if clone.tables[i] == nil {
			return nil, fmt.Errorf("clone for memtable slot %d returned nil", i)
		}
	}
	return clone, nil
}

var _ MemtableManagerIface = (*MemtableManager)(nil)
