package history

import (
	"path"
	"testing"

	"tstore/idgen"
	"tstore/storage"

	"github.com/stretchr/testify/assert"
)

func TestHistory(t *testing.T) {
	rawMap := storage.NewInMemoryMap()
	refGen, err := idgen.New(path.Join("idGens", "refGen"), rawMap, 10)
	assert.Nil(t, err)

	valueHistory, err := New[uint64, string, string](
		"data",
		refGen,
		rawMap,
		func(storagePath string) (ValueHistory[uint64, string, string], error) {
			return NewSingleValueHistory[uint64, string](storagePath, refGen, rawMap)
		})
	assert.Nil(t, err)

	valueHistory.AddVersion(1, CreatedVersionStatus, "Harry")
	valueHistory.AddVersion(2, UpdatedVersionStatus, "Cool")
	valueHistory.AddVersion(3, DeletedVersionStatus, "")
	valueHistory.AddVersion(4, CreatedVersionStatus, "New")

	value1, ok1, err := valueHistory.Value(1)
	assert.Nil(t, err)
	assert.True(t, ok1)
	assert.Equal(t, "Harry", value1)

	value2, ok2, err := valueHistory.Value(2)
	assert.Nil(t, err)
	assert.True(t, ok2)
	assert.Equal(t, "Cool", value2)

	_, ok3, err := valueHistory.Value(3)
	assert.Nil(t, err)
	assert.False(t, ok3)

	value4, ok4, err := valueHistory.Value(4)
	assert.True(t, ok4)
	assert.Equal(t, "New", value4)

	versions := []Version[string]{
		{
			Status: CreatedVersionStatus,
			Value:  value1,
		},
		{
			Status: UpdatedVersionStatus,
			Value:  value2,
		},
		{
			Status: DeletedVersionStatus,
		},
		{
			Status: CreatedVersionStatus,
			Value:  value4,
		},
	}

	versions1, err := valueHistory.ChangesBetween(2, 3)
	assert.Nil(t, err)
	assert.Equal(t, versions[1:3], versions1)

	versions2, err := valueHistory.ChangesBetween(1, 4)
	assert.Nil(t, err)
	assert.Equal(t, versions[0:4], versions2)
}
