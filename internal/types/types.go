package types

import "time"

type Bucket struct {
	Name      string    `json:"name"`
	Policy    string    `json:"policy"`
	CreatedAt time.Time `json:"createdAt"`
}

type Object struct {
	Bucket         string            `json:"bucket"`
	Key            string            `json:"key"`
	Size           int64             `json:"size"`
	ETag           string            `json:"etag"`
	ContentType    string            `json:"contentType"`
	StorageBackend string            `json:"storageBackend"`
	PhysicalPath   string            `json:"-"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	CreatedAt      time.Time         `json:"createdAt"`
	UpdatedAt      time.Time         `json:"updatedAt"`
}

type MultipartUpload struct {
	ID          string    `json:"id"`
	Bucket      string    `json:"bucket"`
	Key         string    `json:"key"`
	ContentType string    `json:"contentType"`
	CreatedAt   time.Time `json:"createdAt"`
}

type MultipartPart struct {
	UploadID   string    `json:"uploadId"`
	PartNumber int       `json:"partNumber"`
	ETag       string    `json:"etag"`
	Size       int64     `json:"size"`
	Path       string    `json:"-"`
	CreatedAt  time.Time `json:"createdAt"`
}

type Stream struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Bucket    string    `json:"bucket"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"createdAt"`
}

type StreamSegment struct {
	StreamID       string    `json:"streamId"`
	Rendition      string    `json:"rendition"`
	SequenceNumber int64     `json:"sequenceNumber"`
	DurationMS     int64     `json:"durationMs"`
	ObjectKey      string    `json:"objectKey"`
	Size           int64     `json:"size"`
	ETag           string    `json:"etag"`
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"createdAt"`
}
