package storage

type InMemoryMap struct {
	data map[string][]byte
}

var _ RawMap = (*InMemoryMap)(nil)

func (i InMemoryMap) Get(key string) ([]byte, error) {
	return i.data[key], nil
}

func (i InMemoryMap) Set(key string, data []byte) error {
	i.data[key] = data
	return nil
}

func (i InMemoryMap) Contain(key string) (bool, error) {
	_, ok := i.data[key]
	return ok, nil
}

func (i InMemoryMap) Delete(key string) error {
	delete(i.data, key)
	return nil
}

func NewInMemoryMap() InMemoryMap {
	return InMemoryMap{
		data: map[string][]byte{},
	}
}
