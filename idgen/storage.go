package idgen

import (
	"encoding/json"

	"tstore/storage"
)

type Storage struct {
	path   string
	rawMap storage.RawMap
}

func (s Storage) writeNextID(nextID uint64) error {
	buf, err := json.Marshal(nextID)
	if err != nil {
		return err
	}

	return s.rawMap.Set(s.path, buf)
}

func (s Storage) readNextID() (uint64, error) {
	buf, err := s.rawMap.Get(s.path)
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

func NewStorage(path string, rawMap storage.RawMap) Storage {
	return Storage{
		path:   path,
		rawMap: rawMap,
	}
}
