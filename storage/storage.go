package storage

import (
	"io/ioutil"
	"os"
	"strings"
)

const DataDir = "userData"
const DefaultFileMode os.FileMode = 0755
const JSONIndent = "  "

func ListFileNames(dir string, ext string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var fileNames []string
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		fileName := file.Name()
		if strings.HasSuffix(fileName, ext) {
			fileNames = append(fileNames, fileName)
		}
	}

	return fileNames, nil
}
