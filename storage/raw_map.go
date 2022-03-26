package storage

import (
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
)

type RawMap interface {
	Get(key string) ([]byte, error)
	Set(key string, data []byte) error
	Contain(key string) (bool, error)
	Delete(key string) error
}

type LocalFileMap struct {
	rootDir string
}

var _ RawMap = (*LocalFileMap)(nil)

func (l LocalFileMap) Get(key string) ([]byte, error) {
	return ioutil.ReadFile(path.Join(l.rootDir, key))
}

func (l LocalFileMap) Set(key string, data []byte) error {
	filePath := path.Join(l.rootDir, key)
	dir := filepath.Dir(filePath)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(filePath, data, os.ModePerm)
}

func (l LocalFileMap) Contain(key string) (bool, error) {
	_, err := os.Stat(path.Join(l.rootDir, key))
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}

func (l LocalFileMap) Delete(key string) error {
	return os.RemoveAll(path.Join(l.rootDir, key))
}

func NewLocalFileMap(rootDir string) *LocalFileMap {
	return &LocalFileMap{
		rootDir: rootDir,
	}
}
