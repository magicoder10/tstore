package server

import (
	"encoding/json"
	"io/ioutil"
	"path/filepath"

	"tstore/storage"
)

var databasesPath = filepath.Join(storage.DataDir, "databases.json")

func readDatabases() ([]string, error) {
	buf, err := ioutil.ReadFile(databasesPath)
	if err != nil {
		return nil, err
	}

	var databases []string
	err = json.Unmarshal(buf, &databases)
	return databases, err
}

func writeDatabases(databases []string) error {
	buf, err := json.MarshalIndent(databases, "", storage.JSONIndent)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(databasesPath, buf, storage.DefaultFileMode)
}
