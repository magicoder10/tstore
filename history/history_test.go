package history

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHistory(t *testing.T) {
	valueHistory := New[uint64, string, string](NewSingleValueHistory[uint64, string]())
	valueHistory.AddVersion(1, CreatedVersionStatus, "Harry")
	valueHistory.AddVersion(2, UpdatedVersionStatus, "Cool")
	valueHistory.AddVersion(3, DeletedVersionStatus, "")
	valueHistory.AddVersion(4, CreatedVersionStatus, "New")

	value1, ok1 := valueHistory.Value(1)
	assert.True(t, ok1)
	assert.Equal(t, "Harry", value1)

	value2, ok2 := valueHistory.Value(2)
	assert.True(t, ok2)
	assert.Equal(t, "Cool", value2)

	_, ok3 := valueHistory.Value(3)
	assert.False(t, ok3)

	value4, ok4 := valueHistory.Value(4)
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

	assert.Equal(t, versions[1:3], valueHistory.ChangesBetween(2, 3))
	assert.Equal(t, versions[0:4], valueHistory.ChangesBetween(1, 4))
}
