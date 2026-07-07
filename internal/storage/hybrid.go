package storage

import (
	"context"
	"io"
	"os"
	"strings"
)

type HybridConfig struct {
	LocalRoot     string
	MultipartRoot string
	NASRoot       string
}

type HybridBackend struct {
	local *LocalDiskBackend
	nas   *LocalDiskBackend
}

func NewHybridBackend(cfg HybridConfig) (*HybridBackend, error) {
	local, err := NewLocalDiskBackend("local", cfg.LocalRoot, cfg.MultipartRoot)
	if err != nil {
		return nil, err
	}
	var nas *LocalDiskBackend
	if strings.TrimSpace(cfg.NASRoot) != "" {
		nas, err = NewLocalDiskBackend("nas", cfg.NASRoot, cfg.MultipartRoot)
		if err != nil {
			return nil, err
		}
	}
	return &HybridBackend{local: local, nas: nas}, nil
}

func (b *HybridBackend) Put(ctx context.Context, object ObjectWrite) (StoredObject, error) {
	if shouldUseNAS(object.Key) && b.nas != nil {
		return b.nas.Put(ctx, object)
	}
	return b.local.Put(ctx, object)
}

func (b *HybridBackend) Get(ctx context.Context, bucket, key string, rangeSpec *RangeSpec) (io.ReadCloser, StoredObject, error) {
	reader, obj, err := b.local.Get(ctx, bucket, key, rangeSpec)
	if err == nil {
		return reader, obj, nil
	}
	if os.IsNotExist(err) && b.nas != nil {
		return b.nas.Get(ctx, bucket, key, rangeSpec)
	}
	return nil, StoredObject{}, err
}

func (b *HybridBackend) Delete(ctx context.Context, bucket, key string) error {
	_ = b.local.Delete(ctx, bucket, key)
	if b.nas != nil {
		_ = b.nas.Delete(ctx, bucket, key)
	}
	return nil
}

func (b *HybridBackend) RenameBucket(ctx context.Context, oldName, newName string) error {
	if err := b.local.RenameBucket(ctx, oldName, newName); err != nil {
		return err
	}
	if b.nas == nil {
		return nil
	}
	return b.nas.RenameBucket(ctx, oldName, newName)
}

func (b *HybridBackend) DeleteBucket(ctx context.Context, bucket string) error {
	_ = b.local.DeleteBucket(ctx, bucket)
	if b.nas != nil {
		_ = b.nas.DeleteBucket(ctx, bucket)
	}
	return nil
}

func (b *HybridBackend) SaveMultipartPart(ctx context.Context, uploadID string, partNumber int, body io.Reader) (StoredObject, error) {
	return b.local.SaveMultipartPart(ctx, uploadID, partNumber, body)
}

func (b *HybridBackend) OpenMultipartPart(ctx context.Context, path string) (io.ReadCloser, error) {
	return b.local.OpenMultipartPart(ctx, path)
}

func (b *HybridBackend) DeleteMultipart(ctx context.Context, uploadID string) error {
	return b.local.DeleteMultipart(ctx, uploadID)
}

func (b *HybridBackend) Health(ctx context.Context) []HealthStatus {
	out := b.local.Health(ctx)
	if b.nas == nil {
		out = append(out, HealthStatus{Name: "nas", Healthy: false, Message: "NAS_DATA_ROOT is not configured"})
		return out
	}
	out = append(out, b.nas.Health(ctx)...)
	return out
}

func shouldUseNAS(key string) bool {
	key = strings.ToLower(key)
	return strings.Contains(key, "/archive/") || strings.Contains(key, "/recordings/")
}
