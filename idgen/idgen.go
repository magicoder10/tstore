package idgen

import (
	"encoding/json"

	"tstore/storage"
)

type IDGen struct {
	storagePath string
	rawMap      storage.RawMap
	bufferSize  int
	nextID      uint64
	nextIDs     chan uint64
	remain      uint64
}

func (i IDGen) NextID() (uint64, error) {
	if len(i.nextIDs) == 0 {
		nextID := i.nextID + uint64(i.bufferSize)
		err := i.writeNextID(nextID)
		if err != nil {
			return 0, err
		}

		for count := 0; count < i.bufferSize; count++ {
			i.nextIDs <- i.nextID
			i.nextID++
		}
	}

	return <-i.nextIDs, nil
}

func (i IDGen) writeNextID(nextID uint64) error {
	buf, err := json.Marshal(nextID)
	if err != nil {
		return err
	}

	return i.rawMap.Set(i.storagePath, buf)
}

func (i IDGen) readNextID() (uint64, error) {
	return readNextID(i.storagePath, i.rawMap)
}

func New(storagePath string, rawMap storage.RawMap, bufferSize int) (IDGen, error) {
	nextID, err := readNextID(storagePath, rawMap)
	if err != nil {
		return IDGen{}, err
	}

	return IDGen{
		storagePath: storagePath,
		rawMap:      rawMap,
		bufferSize:  bufferSize,
		nextID:      nextID,
		nextIDs:     make(chan uint64, bufferSize),
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
