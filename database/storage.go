package database

import (
	"fmt"
	"os"
	"path/filepath"

	"tstore/storage"
)

var databaseDirPathFmt = filepath.Join(storage.DataDir, "databases", "%s")

type Storage struct {
	databaseDirPath string
}

func (s Storage) DeleteAllData() error {
	return os.RemoveAll(s.databaseDirPath)
}

func newStorage(dbName string) Storage {
	return Storage{databaseDirPath: fmt.Sprintf(databaseDirPathFmt, dbName)}
}
