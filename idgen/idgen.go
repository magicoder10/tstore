package idgen

import (
	"encoding/json"
	"log"

	"tstore/storage"
)

type IDGen struct {
	storagePath string
	rawMap      storage.RawMap
	bufferSize  int
	nextID      uint64
	rangeEnd    uint64
}

func (i *IDGen) NextID() (uint64, error) {
	if i.nextID > i.rangeEnd {
		nextID := i.nextID + uint64(i.bufferSize)
		rangeEnd := nextID - 1

		err := i.writeNextID(nextID)
		if err != nil {
			return 0, err
		}

		i.rangeEnd = rangeEnd
	}

	id := i.nextID
	i.nextID++
	log.Printf("[IDGen][NextID] storagePath=%v, nextID=%v\n", i.storagePath, i.nextID)
	return id, nil
}

func (i IDGen) writeNextID(nextID uint64) error {
	return writeNextID(i.storagePath, i.rawMap, nextID)
}

func (i IDGen) readNextID() (uint64, error) {
	return readNextID(i.storagePath, i.rawMap)
}

func New(storagePath string, rawMap storage.RawMap, bufferSize int) (*IDGen, error) {
	exist, err := rawMap.Contain(storagePath)
	if err != nil {
		return nil, err
	}

	if !exist {

	}

	var nextID uint64 = 1
	if exist {
		nextID, err = readNextID(storagePath, rawMap)
		if err != nil {
			return nil, err
		}
	}

	return &IDGen{
		storagePath: storagePath,
		rawMap:      rawMap,
		bufferSize:  bufferSize,
		nextID:      nextID,
		rangeEnd:    nextID - 1,
	}, nil
}

func readNextID(path string, rawMap storage.RawMap) (uint64, error) {
	buf, err := rawMap.Get(path)
	if err != nil {
		return 0, err
	}

	var nextID uint64
	err = json.Unmarshal(buf, &nextID)
	if err != nil {
		return 0, err
	}

	return nextID, nil
}

func writeNextID(path string, rawMap storage.RawMap, nextID uint64) error {
	buf, err := json.Marshal(nextID)
	if err != nil {
		return err
	}

	return rawMap.Set(path, buf)
}
