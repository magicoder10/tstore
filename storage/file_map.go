package storage

import (
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
)

type FileMap struct {
	rootDir string
}

var _ RawMap = (*FileMap)(nil)

func (f FileMap) Get(key string) ([]byte, error) {
	return ioutil.ReadFile(path.Join(f.rootDir, key))
}

func (f FileMap) Set(key string, data []byte) error {
	filePath := path.Join(f.rootDir, key)
	dir := filepath.Dir(filePath)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(filePath, data, os.ModePerm)
}

func (f FileMap) Contain(key string) (bool, error) {
	_, err := os.Stat(path.Join(f.rootDir, key))
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}

func (f FileMap) Delete(key string) error {
	return os.RemoveAll(path.Join(f.rootDir, key))
}

func NewFileMap(rootDir string) *FileMap {
	return &FileMap{
		rootDir: rootDir,
	}
}
