package engine

import (
	"fmt"
	"strings"
	"time"

	"kv-engine/internal/model"
	"kv-engine/internal/probabilistic/simhash"
)

const simHashKeyPrefix = "__internal__:simhash:"

func simHashStorageKey(name string) string {
	return simHashKeyPrefix + name
}

func isInternalSystemKey(key string) bool {
	return strings.HasPrefix(key, simHashKeyPrefix)
}

func (e *Engine) SimHashStore(name, text string) error {
	if name == "" {
		return fmt.Errorf("simhash key is empty")
	}
	if err := e.allowRequest(1); err != nil {
		return err
	}

	sh := simhash.NewSimHash(text)
	payload, err := sh.Serialize()
	if err != nil {
		return err
	}

	e.seq++
	rec := model.Record{
		Key:       simHashStorageKey(name),
		Value:     payload,
		Tombstone: false,
		Seq:       e.seq,
		ExpiresAt: 0,
		Kind:      model.RecordKindKV,
		Structure: model.StructureTypeNone,
		Op:        model.MergeOpNone,
	}
	if err := e.ApplyRecord(rec, false); err != nil {
		return err
	}

	e.putSimHashFingerprintToCache(name, sh.Fingerprint(), rec.Seq)
	return nil
}

func (e *Engine) SimHashGet(name string) (uint64, bool, error) {
	if err := e.allowRequest(1); err != nil {
		return 0, false, err
	}
	return e.simHashGetNoRateLimit(name)
}

func (e *Engine) simHashGetNoRateLimit(name string) (uint64, bool, error) {
	if name == "" {
		return 0, false, fmt.Errorf("simhash key is empty")
	}

	if fp, ok := e.getSimHashFingerprintFromCache(name); ok {
		return fp, true, nil
	}

	internalKey := simHashStorageKey(name)
	now := uint64(time.Now().Unix())

	memRec := e.mem.Get(internalKey)
	if memRec.Found {
		if memRec.Tombstone || (memRec.ExpiresAt > 0 && memRec.ExpiresAt <= now) {
			e.invalidateSimHashCache(name)
			return 0, false, nil
		}
		sh, err := simhash.Deserialize(memRec.Value)
		if err != nil {
			return 0, false, err
		}
		fp := sh.Fingerprint()
		e.putSimHashFingerprintToCache(name, fp, memRec.Seq)
		return fp, true, nil
	}

	val, found, err := e.lsm.Get(internalKey)
	if err != nil {
		return 0, false, err
	}
	if !found {
		e.invalidateSimHashCache(name)
		return 0, false, nil
	}

	sh, err := simhash.Deserialize(val)
	if err != nil {
		return 0, false, err
	}
	fp := sh.Fingerprint()
	e.putSimHashFingerprintToCache(name, fp, 0)
	return fp, true, nil
}

func (e *Engine) SimHashDistance(name1, name2 string) (int, error) {
	if err := e.allowRequest(1); err != nil {
		return 0, err
	}

	fp1, found1, err := e.simHashGetNoRateLimit(name1)
	if err != nil {
		return 0, err
	}
	if !found1 {
		return 0, fmt.Errorf("simhash instance not found: %s", name1)
	}

	fp2, found2, err := e.simHashGetNoRateLimit(name2)
	if err != nil {
		return 0, err
	}
	if !found2 {
		return 0, fmt.Errorf("simhash instance not found: %s", name2)
	}

	return simhash.HammingDistance(fp1, fp2), nil
}
