package s3api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"time"

	"s3store/backend/internal/types"
)

type listAllMyBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Owner   owner    `xml:"Owner"`
	Buckets buckets  `xml:"Buckets"`
}

type owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type buckets struct {
	Bucket []bucketXML `xml:"Bucket"`
}

type bucketXML struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type listBucketResult struct {
	XMLName     xml.Name    `xml:"ListBucketResult"`
	Xmlns       string      `xml:"xmlns,attr"`
	Name        string      `xml:"Name"`
	Prefix      string      `xml:"Prefix"`
	KeyCount    int         `xml:"KeyCount"`
	MaxKeys     int         `xml:"MaxKeys"`
	IsTruncated bool        `xml:"IsTruncated"`
	NextToken   string      `xml:"NextContinuationToken,omitempty"`
	Contents    []objectXML `xml:"Contents"`
}

type objectXML struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type createMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type completeMultipartUploadRequest struct {
	Parts []completePart `xml:"Part"`
}

type completePart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type completeMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

type listPartsResult struct {
	XMLName  xml.Name  `xml:"ListPartsResult"`
	Xmlns    string    `xml:"xmlns,attr"`
	Bucket   string    `xml:"Bucket"`
	Key      string    `xml:"Key"`
	UploadID string    `xml:"UploadId"`
	Parts    []partXML `xml:"Part"`
}

type partXML struct {
	PartNumber   int    `xml:"PartNumber"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
}

func bucketResult(items []types.Bucket) listAllMyBucketsResult {
	out := listAllMyBucketsResult{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
		Owner: owner{ID: "s3store", DisplayName: "s3store"},
	}
	for _, b := range items {
		out.Buckets.Bucket = append(out.Buckets.Bucket, bucketXML{Name: b.Name, CreationDate: formatTime(b.CreatedAt)})
	}
	return out
}

func objectListResult(bucket, prefix string, maxKeys int, next string, objects []types.Object) listBucketResult {
	out := listBucketResult{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", Name: bucket, Prefix: prefix, MaxKeys: maxKeys, KeyCount: len(objects), IsTruncated: next != "", NextToken: next}
	for _, obj := range objects {
		out.Contents = append(out.Contents, objectXML{Key: obj.Key, LastModified: formatTime(obj.UpdatedAt), ETag: `"` + obj.ETag + `"`, Size: obj.Size, StorageClass: "STANDARD"})
	}
	return out
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		t = time.Now().UTC()
	}
	return t.UTC().Format(time.RFC3339)
}

func randomHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "fallback"
	}
	return hex.EncodeToString(buf)
}
