package storage

type RawMap interface {
	Get(key string) ([]byte, error)
	Set(key string, data []byte) error
	Contain(key string) (bool, error)
	Delete(key string) error
}
