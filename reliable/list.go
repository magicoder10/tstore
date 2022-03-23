package reliable

import (
	"encoding/json"
	"errors"
	"path"
	"strconv"

	"tstore/idgen"
	"tstore/storage"
)

type List[Item any] struct {
	path   string
	refGen idgen.IDGen
	rawMap storage.RawMap
}

func (l *List[Item]) Append(item Item) error {
	nodeRefPath, err := l.createNode()
	if err != nil {
		return err
	}

	buf, err := json.Marshal(item)
	if err != nil {
		return err
	}

	nodeDataPath := path.Join(nodeRefPath, "data")
	err = l.rawMap.Set(nodeDataPath, buf)
	if err != nil {
		return err
	}

	tailPath := path.Join(l.path, "tail")
	tailRef, err := l.rawMap.Get(tailPath)
	if err != nil {
		return err
	}

	nodePrevPath := path.Join(nodeRefPath, "prev")
	err = l.rawMap.Set(nodePrevPath, tailRef)
	if err != nil {
		return err
	}

	refBuf, err := json.Marshal(nodeRefPath)
	if err != nil {
		return err
	}

	err = l.rawMap.Set(tailPath, refBuf)
	if err != nil {
		return err
	}

	length, err := l.Length()
	if err != nil {
		return err
	}

	buf, err = json.Marshal(length + 1)
	if err != nil {
		return err
	}

	return l.rawMap.Set(l.lengthPath(), buf)
}

func (l *List[Item]) Peek() (Item, error) {
	length, err := l.Length()
	if err != nil {
		return nil, err
	}

	if length < 1 {
		return nil, errors.New("list must have at least 1 item")
	}

	buf, err := l.rawMap.Get(l.tailPath())
	if err != nil {
		return nil, err
	}

	var item Item
	err = json.Unmarshal(buf, &item)
	return item, err
}

func (l *List) Length() (int, error) {
	lenPath := path.Join(l.path, "length")
	buf, err := l.rawMap.Get(lenPath)
	if err != nil {
		return 0, err
	}

	var length int
	err = json.Unmarshal(buf, &length)
	return length, err
}

func (l *List[Item]) Pop() (Item, error) {
	item, err := l.Peek()
	if err != nil {
		return nil, err
	}

	tailPath := path.Join(l.path, "tail")
	tailRef, err := l.rawMap.Get(tailPath)
	if err != nil {
		return nil, err
	}

	var tailNodeRefPath string
	err = json.Unmarshal(tailRef, &tailNodeRefPath)
	if err != nil {
		return nil, err
	}

	nodePrevPath := path.Join(tailNodeRefPath, "prev")
	buf, err := l.rawMap.Get(nodePrevPath)
	if err != nil {
		return nil, err
	}

	err = l.rawMap.Set(tailPath, buf)
	if err != nil {
		return nil, err
	}

	length, err := l.Length()
	if err != nil {
		return nil, err
	}

	buf, err = json.Marshal(length - 1)
	if err != nil {
		return nil, err
	}

	err = l.rawMap.Set(l.lengthPath(), buf)
	if err != nil {
		return nil, err
	}

	return item, l.rawMap.Delete(tailNodeRefPath)
}

func (l *List[Item]) lengthPath() string {
	return path.Join(l.path, "length")
}

func (l *List[Item]) tailPath() string {
	return path.Join(l.path, "tail")
}

func (l *List[Item]) createNode() (string, error) {
	ref, err := l.refGen.NextID()
	if err != nil {
		return "", err
	}

	return path.Join(l.path, "nodes", strconv.FormatUint(ref, 10)), nil
}
