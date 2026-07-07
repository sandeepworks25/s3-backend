package storage

import (
	"context"
	"io"
)

type ObjectWrite struct {
	Bucket      string
	Key         string
	ContentType string
	Body        io.Reader
}

type StoredObject struct {
	Size           int64
	ETag           string
	ContentType    string
	StorageBackend string
	PhysicalPath   string
}

type RangeSpec struct {
	Start int64
	End   int64
}

type HealthStatus struct {
	Name    string `json:"name"`
	Healthy bool   `json:"healthy"`
	Message string `json:"message"`
}

type Backend interface {
	Put(ctx context.Context, object ObjectWrite) (StoredObject, error)
	Get(ctx context.Context, bucket, key string, rangeSpec *RangeSpec) (io.ReadCloser, StoredObject, error)
	Delete(ctx context.Context, bucket, key string) error
	RenameBucket(ctx context.Context, oldName, newName string) error
	DeleteBucket(ctx context.Context, bucket string) error
	SaveMultipartPart(ctx context.Context, uploadID string, partNumber int, body io.Reader) (StoredObject, error)
	OpenMultipartPart(ctx context.Context, path string) (io.ReadCloser, error)
	DeleteMultipart(ctx context.Context, uploadID string) error
	Health(ctx context.Context) []HealthStatus
}
