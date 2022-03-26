package reliable

import (
	"encoding/json"
	"fmt"
	"log"
	"path"

	"tstore/idgen"
	"tstore/storage"
	"tstore/types"
)

type Map[Key types.Comparable, value any] struct {
	storagePath string
	rawMap      storage.RawMap
	keys        List[Key]
}

func (m Map[Key, Value]) Get(key Key) (Value, error) {
	buf, err := m.rawMap.Get(m.itemKeyPath(key))
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
		log.Println(err)
		return err
	}

	err = m.recordKey(key)
	if err != nil {
		log.Println(err)
		return err
	}

	return m.rawMap.Set(m.itemKeyPath(key), buf)
}

func (m Map[Key, Value]) Delete(key Key) error {
	err := m.rawMap.Delete(m.itemKeyPath(key))
	if err != nil {
		log.Println(err)
		return err
	}

	err = m.cleanUpKey(key)
	if err != nil {
		log.Println(err)
	}

	return err
}

func (m Map[Key, Value]) Contain(key Key) (bool, error) {
	return m.rawMap.Contain(m.itemKeyPath(key))
}

func (m Map[Key, Value]) Keys() ([]Key, error) {
	return m.keys.Items()
}

func (m Map[Key, Value]) recordKey(key Key) error {
	keyPath := m.keyPath(key)
	contain, err := m.rawMap.Contain(keyPath)
	if err != nil {
		log.Println(err)
		return err
	}

	if contain {
		return nil
	}

	nodeRef, err := m.keys.append(key)
	if err != nil {
		log.Println(err)
		return err
	}

	buf, err := json.Marshal(nodeRef)
	if err != nil {
		log.Println(err)
		return err
	}

	err = m.rawMap.Set(keyPath, buf)
	if err != nil {
		log.Println(err)
	}

	return err
}

func (m Map[Key, Value]) cleanUpKey(key Key) error {
	keyPath := m.keyPath(key)
	contain, err := m.rawMap.Contain(keyPath)
	if err != nil {
		log.Println(err)
		return err
	}

	if !contain {
		return nil
	}

	buf, err := m.rawMap.Get(keyPath)
	if err != nil {
		log.Println(err)
		return err
	}

	var nodeRef string
	err = json.Unmarshal(buf, &nodeRef)
	if err != nil {
		log.Println(err)
		return err
	}

	err = m.keys.delete(nodeRef)
	if err != nil {
		log.Println(err)
	}

	return err
}

func (m Map[Key, Value]) keyPath(key Key) string {
	return path.Join(m.storagePath, "keys", fmt.Sprintf("%v", key))
}

func (m Map[Key, Value]) itemKeyPath(key Key) string {
	return path.Join(m.storagePath, "pairs", fmt.Sprintf("%v", key))
}

func NewMap[Key types.Comparable, Value any](
	storagePath string,
	refGen *idgen.IDGen,
	rawMap storage.RawMap) (Map[Key, Value], error) {
	keys, err := NewList[Key](path.Join(storagePath, "keys"), refGen, rawMap)
	if err != nil {
		return *new(Map[Key, Value]), err
	}

	return Map[Key, Value]{
		storagePath: storagePath,
		rawMap:      rawMap,
		keys:        keys,
	}, nil
}
