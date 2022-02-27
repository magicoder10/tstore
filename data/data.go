package data

type Data struct {
	// Use iterators for big data
	// Support indexing for quick data lookup
	schemas  map[string]Schema // key: schema name
	entities map[uint64]Entity // key: entityID
}
