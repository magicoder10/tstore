package idgen

type IDGen struct {
	storage    Storage
	bufferSize int
	nextID     uint64
	nextIDs    chan uint64
	remain     uint64
}

func (i IDGen) NextID() (uint64, error) {
	if len(i.nextIDs) == 0 {
		nextID := i.nextID + uint64(i.bufferSize)
		err := i.storage.writeNextID(nextID)
		if err != nil {
			return 0, err
		}

		for count := 0; count < i.bufferSize; count++ {
			i.nextIDs <- i.nextID
			i.nextID++
		}
	}

	return <-i.nextIDs, nil
}

func New(storage Storage, bufferSize int) (IDGen, error) {
	nextID, err := storage.readNextID()
	if err != nil {
		return IDGen{}, err
	}

	return IDGen{
		storage:    storage,
		bufferSize: bufferSize,
		nextID:     nextID,
		nextIDs:    make(chan uint64, bufferSize),
	}, nil
}
