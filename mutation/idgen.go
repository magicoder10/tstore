package mutation

import (
	"fmt"
	"os"

	"tstore/storage"
)

const idGenBuffer = 10

type IDGen struct {
	storage IDGenStorage
	nextID  uint64
	nextIDs chan uint64
	remain  uint64
}

func (i IDGen) NextID() uint64 {
	if len(i.nextIDs) == 0 {
		for count := 0; count < idGenBuffer; count++ {
			i.nextIDs <- i.nextID
			i.nextID++
		}

		i.storage.WriteNextID(i.nextID)
	}

	return <-i.nextIDs
}

func newIDGen(dbName string, idGenName string) (IDGen, error) {
	idGenDirPath := fmt.Sprintf(idGenDirFmt, dbName)
	err := os.MkdirAll(idGenDirPath, storage.DefaultFileMode)
	if err != nil {
		return IDGen{}, err
	}

	idGenPath := fmt.Sprintf(idGenPathFmt, idGenDirPath, idGenName)
	idGenStorage := NewIDGenStorage(idGenPath)
	nextID, err := idGenStorage.ReadNextID()
	if err != nil {
		return IDGen{}, err
	}

	return IDGen{
		storage: idGenStorage,
		nextID:  nextID,
		nextIDs: make(chan uint64, idGenBuffer),
	}, nil
}
