package reliable

import (
	"encoding/json"
	"fmt"
	"path"

	"tstore/storage"
	"tstore/types"
)

type Map[Key types.Comparable, value any] struct {
	storagePath string
	rawMap      storage.RawMap
}

func (m Map[Key, Value]) Get(key Key) (Value, error) {
	buf, err := m.rawMap.Get(path.Join(m.storagePath, fmt.Sprintf("%v", key)))
	if err != nil {
		return *new(Value), err
	}

	var value Value
	err = json.Unmarshal(buf, &value)
	return value, err
}

func (m Map[Key, Value]) Set(key Key, value Value) error {
	buf, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return m.rawMap.Set(path.Join(m.storagePath, fmt.Sprintf("%v", key)), buf)
}

func (m Map[Key, Value]) Delete(key Key) error {
	return m.rawMap.Delete(path.Join(m.storagePath, fmt.Sprintf("%v", key)))
}

func (m Map[Key, Value]) Contain(key Key) (bool, error) {
	return m.rawMap.Contain(path.Join(m.storagePath, fmt.Sprintf("%v", key)))
}

func NewMap[Key types.Comparable, Value any](storagePath string, rawMap storage.RawMap) Map[Key, Value] {
	return Map[Key, Value]{
		storagePath: storagePath,
		rawMap:      rawMap,
	}
}
