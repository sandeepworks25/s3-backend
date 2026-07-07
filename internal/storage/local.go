package storage

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type LocalDiskBackend struct {
	name          string
	root          string
	multipartRoot string
}

func NewLocalDiskBackend(name, root, multipartRoot string) (*LocalDiskBackend, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(multipartRoot, 0o755); err != nil {
		return nil, err
	}
	return &LocalDiskBackend{name: name, root: root, multipartRoot: multipartRoot}, nil
}

func (b *LocalDiskBackend) Put(_ context.Context, object ObjectWrite) (StoredObject, error) {
	finalPath, err := safeObjectPath(b.root, object.Bucket, object.Key)
	if err != nil {
		return StoredObject{}, err
	}
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		return StoredObject{}, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(finalPath), ".upload-*")
	if err != nil {
		return StoredObject{}, err
	}
	tmpPath := tmp.Name()
	hasher := md5.New()
	size, copyErr := io.Copy(io.MultiWriter(tmp, hasher), object.Body)
	closeErr := tmp.Close()
	if copyErr != nil {
		_ = os.Remove(tmpPath)
		return StoredObject{}, copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return StoredObject{}, closeErr
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return StoredObject{}, err
	}
	contentType := object.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	return StoredObject{
		Size:           size,
		ETag:           hex.EncodeToString(hasher.Sum(nil)),
		ContentType:    contentType,
		StorageBackend: b.name,
		PhysicalPath:   finalPath,
	}, nil
}

func (b *LocalDiskBackend) Get(_ context.Context, bucket, key string, rangeSpec *RangeSpec) (io.ReadCloser, StoredObject, error) {
	path, err := safeObjectPath(b.root, bucket, key)
	if err != nil {
		return nil, StoredObject{}, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, StoredObject{}, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, StoredObject{}, err
	}
	if rangeSpec != nil {
		if _, err := file.Seek(rangeSpec.Start, io.SeekStart); err != nil {
			_ = file.Close()
			return nil, StoredObject{}, err
		}
		length := rangeSpec.End - rangeSpec.Start + 1
		return struct {
			io.Reader
			io.Closer
		}{Reader: io.LimitReader(file, length), Closer: file}, StoredObject{Size: info.Size(), StorageBackend: b.name, PhysicalPath: path}, nil
	}
	return file, StoredObject{Size: info.Size(), StorageBackend: b.name, PhysicalPath: path}, nil
}

func (b *LocalDiskBackend) Delete(_ context.Context, bucket, key string) error {
	path, err := safeObjectPath(b.root, bucket, key)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (b *LocalDiskBackend) RenameBucket(_ context.Context, oldName, newName string) error {
	oldPath, err := safePath(b.root, oldName)
	if err != nil {
		return err
	}
	newPath, err := safePath(b.root, newName)
	if err != nil {
		return err
	}
	if _, err := os.Stat(oldPath); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(newPath), 0o755); err != nil {
		return err
	}
	return os.Rename(oldPath, newPath)
}

func (b *LocalDiskBackend) DeleteBucket(_ context.Context, bucket string) error {
	path, err := safePath(b.root, bucket)
	if err != nil {
		return err
	}
	return os.RemoveAll(path)
}

func (b *LocalDiskBackend) SaveMultipartPart(_ context.Context, uploadID string, partNumber int, body io.Reader) (StoredObject, error) {
	dir, err := safePath(b.multipartRoot, uploadID)
	if err != nil {
		return StoredObject{}, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return StoredObject{}, err
	}
	path := filepath.Join(dir, fmt.Sprintf("%05d.part", partNumber))
	tmp, err := os.CreateTemp(dir, ".part-*")
	if err != nil {
		return StoredObject{}, err
	}
	tmpPath := tmp.Name()
	hasher := md5.New()
	size, copyErr := io.Copy(io.MultiWriter(tmp, hasher), body)
	closeErr := tmp.Close()
	if copyErr != nil {
		_ = os.Remove(tmpPath)
		return StoredObject{}, copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return StoredObject{}, closeErr
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return StoredObject{}, err
	}
	return StoredObject{Size: size, ETag: hex.EncodeToString(hasher.Sum(nil)), StorageBackend: b.name, PhysicalPath: path}, nil
}

func (b *LocalDiskBackend) OpenMultipartPart(_ context.Context, path string) (io.ReadCloser, error) {
	cleanRoot, _ := filepath.Abs(b.multipartRoot)
	cleanPath, _ := filepath.Abs(path)
	if !strings.HasPrefix(cleanPath, cleanRoot) {
		return nil, errors.New("multipart part path escapes root")
	}
	return os.Open(cleanPath)
}

func (b *LocalDiskBackend) DeleteMultipart(_ context.Context, uploadID string) error {
	dir, err := safePath(b.multipartRoot, uploadID)
	if err != nil {
		return err
	}
	return os.RemoveAll(dir)
}

func (b *LocalDiskBackend) Health(context.Context) []HealthStatus {
	if err := os.MkdirAll(b.root, 0o755); err != nil {
		return []HealthStatus{{Name: b.name, Healthy: false, Message: err.Error()}}
	}
	test := filepath.Join(b.root, ".health")
	if err := os.WriteFile(test, []byte("ok"), 0o644); err != nil {
		return []HealthStatus{{Name: b.name, Healthy: false, Message: err.Error()}}
	}
	_ = os.Remove(test)
	return []HealthStatus{{Name: b.name, Healthy: true, Message: "ok"}}
}

func ParseRange(header string, size int64) (*RangeSpec, bool) {
	if header == "" || !strings.HasPrefix(header, "bytes=") {
		return nil, false
	}
	raw := strings.TrimSpace(strings.TrimPrefix(header, "bytes="))
	if strings.Contains(raw, ",") {
		return nil, false
	}
	startRaw, endRaw, ok := strings.Cut(raw, "-")
	if !ok {
		return nil, false
	}
	var start, end int64
	if startRaw == "" {
		suffix, err := strconv.ParseInt(endRaw, 10, 64)
		if err != nil || suffix <= 0 {
			return nil, false
		}
		if suffix > size {
			suffix = size
		}
		start = size - suffix
		end = size - 1
	} else {
		var err error
		start, err = strconv.ParseInt(startRaw, 10, 64)
		if err != nil {
			return nil, false
		}
		if endRaw == "" {
			end = size - 1
		} else {
			end, err = strconv.ParseInt(endRaw, 10, 64)
			if err != nil {
				return nil, false
			}
		}
	}
	if start < 0 || end < start || end >= size || size <= 0 {
		return nil, false
	}
	return &RangeSpec{Start: start, End: end}, true
}

func DetectContentType(name string, sniff []byte) string {
	if c := http.DetectContentType(sniff); c != "application/octet-stream" {
		return c
	}
	ext := strings.ToLower(filepath.Ext(name))
	switch ext {
	case ".m3u8":
		return "application/vnd.apple.mpegurl"
	case ".m4s":
		return "video/iso.segment"
	case ".mp4":
		return "video/mp4"
	case ".ts":
		return "video/mp2t"
	default:
		return "application/octet-stream"
	}
}

func safeObjectPath(root, bucket, key string) (string, error) {
	if bucket == "" || key == "" {
		return "", errors.New("bucket and key are required")
	}
	return safePath(root, filepath.Join(bucket, filepath.FromSlash(key)))
}

func safePath(root, child string) (string, error) {
	cleanRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	cleanPath, err := filepath.Abs(filepath.Join(cleanRoot, child))
	if err != nil {
		return "", err
	}
	if cleanPath != cleanRoot && !strings.HasPrefix(cleanPath, cleanRoot+string(os.PathSeparator)) {
		return "", errors.New("path escapes storage root")
	}
	return cleanPath, nil
}
