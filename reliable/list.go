package reliable

import (
	"encoding/json"
	"errors"
	"log"
	"path"
	"strconv"

	"tstore/idgen"
	"tstore/storage"
)

type List[Item any] struct {
	storagePath string
	refGen      *idgen.IDGen
	rawMap      storage.RawMap
}

func (l *List[Item]) Append(item Item) error {
	_, err := l.append(item)
	return err
}

func (l *List[Item]) Peek() (Item, error) {
	length, err := l.Length()
	if err != nil {
		log.Println(err)
		return *new(Item), err
	}

	if length < 1 {
		return *new(Item), errors.New("list must have at least 1 item")
	}

	nodeRefPath, err := l.getNodeRefPath(l.tailPath())
	if err != nil {
		log.Println(err)
		return *new(Item), err
	}

	nodeDataPath := path.Join(nodeRefPath, "data")
	buf, err := l.rawMap.Get(nodeDataPath)
	if err != nil {
		log.Println(err)
		return *new(Item), err
	}

	var item Item
	err = json.Unmarshal(buf, &item)
	return item, err
}

func (l *List[Item]) Length() (int, error) {
	lenPath := l.lengthPath()
	contain, err := l.rawMap.Contain(lenPath)
	if err != nil {
		log.Println(err)
		return 0, err
	}

	if !contain {
		return 0, nil
	}

	buf, err := l.rawMap.Get(lenPath)
	if err != nil {
		log.Println(err)
		return 0, err
	}

	var length int
	err = json.Unmarshal(buf, &length)
	return length, err
}

func (l *List[Item]) Pop() (Item, error) {
	item, err := l.Peek()
	if err != nil {
		log.Println(err)
		return *new(Item), err
	}

	tailNodeRefPath, err := l.getNodeRefPath(l.tailPath())
	if err != nil {
		log.Println(err)
		return *new(Item), err
	}

	nodePrevPath, err := l.getNodeRefPath(path.Join(tailNodeRefPath, "prev"))
	if err != nil {
		log.Println(err)
		return *new(Item), err
	}

	err = l.rawMap.Delete(path.Join(nodePrevPath, "next"))
	if err != nil {
		log.Println(err)
		return *new(Item), err
	}

	err = l.decrementLength()
	if err != nil {
		log.Println(err)
		return *new(Item), err
	}

	return item, l.rawMap.Delete(tailNodeRefPath)
}

func (l List[Item]) Items() ([]Item, error) {
	// TODO: use iterator instead
	dummyPath := path.Join(l.storagePath, "dummy")
	buf, err := l.rawMap.Get(dummyPath)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	var prevPath string
	err = json.Unmarshal(buf, &prevPath)
	if err != nil {
		return nil, err
	}

	items := make([]Item, 0)

	for true {
		nextPath := path.Join(prevPath, "next")
		contains, err := l.rawMap.Contain(nextPath)
		if err != nil {
			log.Println(err)
			return nil, err
		}

		if !contains {
			break
		}

		currNodeRef, err := l.getNodeRefPath(nextPath)
		if err != nil {
			log.Println(err)
			return nil, err
		}

		item, err := l.getItem(currNodeRef)
		if err != nil {
			log.Println(err)
			return nil, err
		}

		items = append(items, item)
		prevPath = currNodeRef
	}

	return items, nil
}

func (l *List[Item]) append(item Item) (string, error) {
	nodeRefPath, err := l.createNode()
	if err != nil {
		log.Println(err)
		return "", err
	}

	buf, err := json.Marshal(item)
	if err != nil {
		log.Println(err)
		return "", err
	}

	nodeDataPath := path.Join(nodeRefPath, "data")
	err = l.rawMap.Set(nodeDataPath, buf)
	if err != nil {
		log.Println(err)
		return "", err
	}

	tailPath := l.tailPath()
	tailRefBuf, err := l.rawMap.Get(tailPath)
	if err != nil {
		log.Println(err)
		return "", err
	}

	nodePrevPath := path.Join(nodeRefPath, "prev")
	err = l.rawMap.Set(nodePrevPath, tailRefBuf)
	if err != nil {
		log.Println(err)
		return "", err
	}

	nodeRefBuf, err := json.Marshal(nodeRefPath)
	if err != nil {
		log.Println(err)
		return "", err
	}

	err = l.rawMap.Set(tailPath, nodeRefBuf)
	if err != nil {
		log.Println(err)
		return "", err
	}

	var tailNodeRef string
	err = json.Unmarshal(tailRefBuf, &tailNodeRef)
	if err != nil {
		log.Println(err)
		return "", err
	}

	tailNextRefPath := path.Join(tailNodeRef, "next")
	err = l.rawMap.Set(tailNextRefPath, nodeRefBuf)
	if err != nil {
		log.Println(err)
		return "", err
	}

	length, err := l.Length()
	if err != nil {
		log.Println(err)
		return "", err
	}

	buf, err = json.Marshal(length + 1)
	if err != nil {
		log.Println(err)
		return "", err
	}

	return nodeRefPath, l.rawMap.Set(l.lengthPath(), buf)
}

func (l *List[Item]) delete(nodePath string) error {
	contain, err := l.rawMap.Contain(nodePath)
	if err != nil {
		log.Println(err)
		return err
	}

	if !contain {
		return nil
	}

	prevBuf, err := l.rawMap.Get(path.Join(nodePath, "prev"))
	if err != nil {
		log.Println(err)
		return err
	}

	var nodePrevPath string
	err = json.Unmarshal(prevBuf, &nodePrevPath)
	if err != nil {
		log.Println(err)
	}

	nextBuf, err := l.rawMap.Get(path.Join(nodePath, "next"))
	if err != nil {
		log.Println(err)
		return err
	}

	var nodeNextPath string
	err = json.Unmarshal(prevBuf, &nodePrevPath)
	if err != nil {
		log.Println(err)
		return err
	}

	err = l.rawMap.Set(path.Join(nodePrevPath, "next"), nextBuf)
	if err != nil {
		log.Println(err)
		return err
	}

	err = l.rawMap.Set(path.Join(nodeNextPath, "prev"), prevBuf)
	if err != nil {
		log.Println(err)
		return err
	}

	err = l.decrementLength()
	if err != nil {
		log.Println(err)
		return err
	}

	return l.rawMap.Delete(nodePath)
}

func (l *List[Item]) decrementLength() error {
	length, err := l.Length()
	if err != nil {
		log.Println(err)
		return err
	}

	buf, err := json.Marshal(length - 1)
	if err != nil {
		log.Println(err)
		return err
	}

	err = l.rawMap.Set(l.lengthPath(), buf)
	if err != nil {
		log.Println(err)
		return err
	}

	return err
}

func (l *List[Item]) getNodeRefPath(refPath string) (string, error) {
	buf, err := l.rawMap.Get(refPath)
	if err != nil {
		log.Println(err)
		return "", err
	}

	var nodeRefPath string
	err = json.Unmarshal(buf, &nodeRefPath)
	if err != nil {
		log.Println(err)
	}

	return nodeRefPath, err
}

func (l *List[Item]) lengthPath() string {
	return path.Join(l.storagePath, "length")
}

func (l *List[Item]) tailPath() string {
	return path.Join(l.storagePath, "tail")
}

func (l *List[Item]) createNode() (string, error) {
	return createNode(l.refGen, l.storagePath)
}

func (l *List[Item]) getItem(nodeRefPath string) (Item, error) {
	nodeDataPath := path.Join(nodeRefPath, "data")
	buf, err := l.rawMap.Get(nodeDataPath)
	if err != nil {
		log.Println(err)
		return *new(Item), err
	}

	var item Item
	err = json.Unmarshal(buf, &item)
	return item, err
}

func NewList[Item any](storagePath string, refGen *idgen.IDGen, rawMap storage.RawMap) (List[Item], error) {
	err := initRefs[Item](storagePath, refGen, rawMap)
	if err != nil {
		return List[Item]{}, err
	}

	return List[Item]{
		storagePath: storagePath,
		refGen:      refGen,
		rawMap:      rawMap,
	}, nil
}

func initRefs[Item any](storagePath string, refGen *idgen.IDGen, rawMap storage.RawMap) error {
	tailPath := path.Join(storagePath, "tail")
	contains, err := rawMap.Contain(tailPath)
	if err != nil {
		log.Println(err)
		return err
	}

	if contains {
		return nil
	}

	nodeRefPath, err := createNode(refGen, storagePath)
	if err != nil {
		log.Println(err)
		return err
	}

	refBuf, err := json.Marshal(nodeRefPath)
	if err != nil {
		log.Println(err)
		return err
	}

	err = rawMap.Set(tailPath, refBuf)
	if err != nil {
		log.Println(err)
	}

	dummyPath := path.Join(storagePath, "dummy")
	err = rawMap.Set(dummyPath, refBuf)
	if err != nil {
		log.Println(err)
	}

	return err
}

func createNode(refGen *idgen.IDGen, listPath string) (string, error) {
	ref, err := refGen.NextID()
	if err != nil {
		return "", err
	}

	return path.Join(listPath, "nodes", strconv.FormatUint(ref, 10)), nil
}
