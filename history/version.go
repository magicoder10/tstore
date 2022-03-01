package history

type VersionStatus string

const (
	CreatedVersionStatus VersionStatus = "created"
	UpdatedVersionStatus VersionStatus = "updated"
	DeletedVersionStatus VersionStatus = "deleted"
)

type Version[Value any] struct {
	Status VersionStatus `json:"status"`
	Value  Value         `json:"value"`
}
