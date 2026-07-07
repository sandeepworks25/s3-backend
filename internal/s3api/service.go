package s3api

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"s3store/backend/internal/auth"
	"s3store/backend/internal/meta"
	"s3store/backend/internal/redisx"
	"s3store/backend/internal/storage"
	"s3store/backend/internal/types"
)

type Service struct {
	repo     meta.Repository
	store    storage.Backend
	verifier *auth.SigV4Verifier
	cache    *redisx.Client
	logger   *slog.Logger
}

func NewService(repo meta.Repository, store storage.Backend, keys auth.KeyStore, cache *redisx.Client, logger *slog.Logger) *Service {
	return &Service{repo: repo, store: store, verifier: auth.NewSigV4Verifier(keys), cache: cache, logger: logger}
}

func (s *Service) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(s.s3Auth)
	r.Get("/", s.listBuckets)
	r.Put("/{bucket}", s.createBucket)
	r.Head("/{bucket}", s.headBucket)
	r.Delete("/{bucket}", s.deleteBucket)
	r.Get("/{bucket}", s.listObjects)
	r.Put("/{bucket}/*", s.putObjectOrPart)
	r.Post("/{bucket}/*", s.postObject)
	r.Get("/{bucket}/*", s.getObjectOrParts)
	r.Head("/{bucket}/*", s.headObject)
	r.Delete("/{bucket}/*", s.deleteObjectOrUpload)
	return r
}

func (s *Service) s3Auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := s.verifier.Verify(r.Context(), r); err != nil {
			writeS3Error(w, http.StatusForbidden, "AccessDenied", err.Error(), r.URL.Path)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Service) listBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := s.repo.ListBuckets(r.Context())
	if err != nil {
		writeS3Error(w, 500, "InternalError", err.Error(), r.URL.Path)
		return
	}
	writeXML(w, http.StatusOK, bucketResult(buckets))
}

func (s *Service) createBucket(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	if _, err := s.repo.CreateBucket(r.Context(), bucket, "private"); err != nil {
		status, code := mapRepoErr(err)
		writeS3Error(w, status, code, err.Error(), r.URL.Path)
		return
	}
	_ = s.repo.AddAudit(r.Context(), "s3", "CreateBucket", bucket, "success")
	w.WriteHeader(http.StatusOK)
}

func (s *Service) headBucket(w http.ResponseWriter, r *http.Request) {
	if _, err := s.repo.GetBucket(r.Context(), chi.URLParam(r, "bucket")); err != nil {
		status, code := mapRepoErr(err)
		writeS3Error(w, status, code, err.Error(), r.URL.Path)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Service) deleteBucket(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	if err := s.repo.DeleteBucket(r.Context(), bucket); err != nil {
		status, code := mapRepoErr(err)
		writeS3Error(w, status, code, err.Error(), r.URL.Path)
		return
	}
	_ = s.repo.AddAudit(r.Context(), "s3", "DeleteBucket", bucket, "success")
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) listObjects(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	prefix := r.URL.Query().Get("prefix")
	cursor := r.URL.Query().Get("continuation-token")
	limit := intQuery(r, "max-keys", 1000)
	objects, next, err := s.repo.ListObjects(r.Context(), bucket, prefix, limit, cursor)
	if err != nil {
		status, code := mapRepoErr(err)
		writeS3Error(w, status, code, err.Error(), r.URL.Path)
		return
	}
	writeXML(w, http.StatusOK, objectListResult(bucket, prefix, limit, next, objects))
}

func (s *Service) putObjectOrPart(w http.ResponseWriter, r *http.Request) {
	if uploadID := r.URL.Query().Get("uploadId"); uploadID != "" {
		s.uploadPart(w, r, uploadID)
		return
	}
	s.putObject(w, r)
}

func (s *Service) putObject(w http.ResponseWriter, r *http.Request) {
	bucket, key := chi.URLParam(r, "bucket"), chi.URLParam(r, "*")
	if _, err := s.repo.GetBucket(r.Context(), bucket); err != nil {
		status, code := mapRepoErr(err)
		writeS3Error(w, status, code, err.Error(), r.URL.Path)
		return
	}
	stored, err := s.store.Put(r.Context(), storage.ObjectWrite{Bucket: bucket, Key: key, ContentType: r.Header.Get("Content-Type"), Body: r.Body})
	if err != nil {
		writeS3Error(w, 500, "InternalError", err.Error(), r.URL.Path)
		return
	}
	obj := types.Object{Bucket: bucket, Key: key, Size: stored.Size, ETag: stored.ETag, ContentType: stored.ContentType, StorageBackend: stored.StorageBackend, PhysicalPath: stored.PhysicalPath, Metadata: userMetadata(r)}
	if err := s.repo.UpsertObject(r.Context(), obj); err != nil {
		writeS3Error(w, 500, "InternalError", err.Error(), r.URL.Path)
		return
	}
	s.cache.Del(r.Context(), "object:"+bucket+":"+key+":head")
	w.Header().Set("ETag", `"`+stored.ETag+`"`)
	w.WriteHeader(http.StatusOK)
}

func (s *Service) getObjectOrParts(w http.ResponseWriter, r *http.Request) {
	if uploadID := r.URL.Query().Get("uploadId"); uploadID != "" {
		s.listParts(w, r, uploadID)
		return
	}
	s.getObject(w, r)
}

func (s *Service) getObject(w http.ResponseWriter, r *http.Request) {
	bucket, key := chi.URLParam(r, "bucket"), chi.URLParam(r, "*")
	obj, err := s.repo.GetObject(r.Context(), bucket, key)
	if err != nil {
		status, code := mapRepoErr(err)
		writeS3Error(w, status, code, err.Error(), r.URL.Path)
		return
	}
	rangeSpec, partial := storage.ParseRange(r.Header.Get("Range"), obj.Size)
	reader, _, err := s.store.Get(r.Context(), bucket, key, rangeSpec)
	if err != nil {
		writeS3Error(w, 404, "NoSuchKey", err.Error(), r.URL.Path)
		return
	}
	defer reader.Close()
	w.Header().Set("Content-Type", obj.ContentType)
	w.Header().Set("ETag", `"`+obj.ETag+`"`)
	w.Header().Set("Accept-Ranges", "bytes")
	for k, v := range obj.Metadata {
		w.Header().Set("x-amz-meta-"+k, v)
	}
	if partial {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rangeSpec.Start, rangeSpec.End, obj.Size))
		w.Header().Set("Content-Length", strconv.FormatInt(rangeSpec.End-rangeSpec.Start+1, 10))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	}
	_, _ = io.Copy(w, reader)
}

func (s *Service) headObject(w http.ResponseWriter, r *http.Request) {
	bucket, key := chi.URLParam(r, "bucket"), chi.URLParam(r, "*")
	obj, err := s.repo.GetObject(r.Context(), bucket, key)
	if err != nil {
		status, code := mapRepoErr(err)
		writeS3Error(w, status, code, err.Error(), r.URL.Path)
		return
	}
	w.Header().Set("Content-Type", obj.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	w.Header().Set("ETag", `"`+obj.ETag+`"`)
	w.WriteHeader(http.StatusOK)
}

func (s *Service) deleteObjectOrUpload(w http.ResponseWriter, r *http.Request) {
	if uploadID := r.URL.Query().Get("uploadId"); uploadID != "" {
		_ = s.repo.DeleteMultipartUpload(r.Context(), uploadID)
		_ = s.store.DeleteMultipart(r.Context(), uploadID)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	bucket, key := chi.URLParam(r, "bucket"), chi.URLParam(r, "*")
	_ = s.store.Delete(r.Context(), bucket, key)
	if err := s.repo.DeleteObject(r.Context(), bucket, key); err != nil {
		status, code := mapRepoErr(err)
		writeS3Error(w, status, code, err.Error(), r.URL.Path)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) postObject(w http.ResponseWriter, r *http.Request) {
	if _, ok := r.URL.Query()["uploads"]; ok {
		s.createMultipartUpload(w, r)
		return
	}
	if uploadID := r.URL.Query().Get("uploadId"); uploadID != "" {
		s.completeMultipartUpload(w, r, uploadID)
		return
	}
	writeS3Error(w, http.StatusBadRequest, "InvalidRequest", "unsupported POST operation", r.URL.Path)
}

func (s *Service) createMultipartUpload(w http.ResponseWriter, r *http.Request) {
	bucket, key := chi.URLParam(r, "bucket"), chi.URLParam(r, "*")
	upload := types.MultipartUpload{ID: randomHex(16), Bucket: bucket, Key: key, ContentType: r.Header.Get("Content-Type"), CreatedAt: time.Now().UTC()}
	if upload.ContentType == "" {
		upload.ContentType = "application/octet-stream"
	}
	if err := s.repo.CreateMultipartUpload(r.Context(), upload); err != nil {
		status, code := mapRepoErr(err)
		writeS3Error(w, status, code, err.Error(), r.URL.Path)
		return
	}
	writeXML(w, http.StatusOK, createMultipartUploadResult{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", Bucket: bucket, Key: key, UploadID: upload.ID})
}

func (s *Service) uploadPart(w http.ResponseWriter, r *http.Request, uploadID string) {
	partNumber := intQuery(r, "partNumber", 0)
	if partNumber <= 0 {
		writeS3Error(w, http.StatusBadRequest, "InvalidPart", "partNumber is required", r.URL.Path)
		return
	}
	if _, err := s.repo.GetMultipartUpload(r.Context(), uploadID); err != nil {
		status, code := mapRepoErr(err)
		writeS3Error(w, status, code, err.Error(), r.URL.Path)
		return
	}
	stored, err := s.store.SaveMultipartPart(r.Context(), uploadID, partNumber, r.Body)
	if err != nil {
		writeS3Error(w, 500, "InternalError", err.Error(), r.URL.Path)
		return
	}
	part := types.MultipartPart{UploadID: uploadID, PartNumber: partNumber, ETag: stored.ETag, Size: stored.Size, Path: stored.PhysicalPath, CreatedAt: time.Now().UTC()}
	if err := s.repo.SaveMultipartPart(r.Context(), part); err != nil {
		writeS3Error(w, 500, "InternalError", err.Error(), r.URL.Path)
		return
	}
	w.Header().Set("ETag", `"`+stored.ETag+`"`)
	w.WriteHeader(http.StatusOK)
}

func (s *Service) listParts(w http.ResponseWriter, r *http.Request, uploadID string) {
	upload, err := s.repo.GetMultipartUpload(r.Context(), uploadID)
	if err != nil {
		status, code := mapRepoErr(err)
		writeS3Error(w, status, code, err.Error(), r.URL.Path)
		return
	}
	parts, err := s.repo.ListMultipartParts(r.Context(), uploadID)
	if err != nil {
		writeS3Error(w, 500, "InternalError", err.Error(), r.URL.Path)
		return
	}
	out := listPartsResult{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", Bucket: upload.Bucket, Key: upload.Key, UploadID: uploadID}
	for _, p := range parts {
		out.Parts = append(out.Parts, partXML{PartNumber: p.PartNumber, LastModified: formatTime(p.CreatedAt), ETag: `"` + p.ETag + `"`, Size: p.Size})
	}
	writeXML(w, http.StatusOK, out)
}

func (s *Service) completeMultipartUpload(w http.ResponseWriter, r *http.Request, uploadID string) {
	upload, err := s.repo.GetMultipartUpload(r.Context(), uploadID)
	if err != nil {
		status, code := mapRepoErr(err)
		writeS3Error(w, status, code, err.Error(), r.URL.Path)
		return
	}
	var req completeMultipartUploadRequest
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		writeS3Error(w, http.StatusBadRequest, "MalformedXML", err.Error(), r.URL.Path)
		return
	}
	parts, err := s.repo.ListMultipartParts(r.Context(), uploadID)
	if err != nil {
		writeS3Error(w, 500, "InternalError", err.Error(), r.URL.Path)
		return
	}
	readers, closeAll, err := s.openRequestedParts(r.Context(), parts, req.Parts)
	if err != nil {
		writeS3Error(w, http.StatusBadRequest, "InvalidPart", err.Error(), r.URL.Path)
		return
	}
	defer closeAll()
	stored, err := s.store.Put(r.Context(), storage.ObjectWrite{Bucket: upload.Bucket, Key: upload.Key, ContentType: upload.ContentType, Body: io.MultiReader(readers...)})
	if err != nil {
		writeS3Error(w, 500, "InternalError", err.Error(), r.URL.Path)
		return
	}
	obj := types.Object{Bucket: upload.Bucket, Key: upload.Key, Size: stored.Size, ETag: stored.ETag, ContentType: upload.ContentType, StorageBackend: stored.StorageBackend, PhysicalPath: stored.PhysicalPath}
	if err := s.repo.UpsertObject(r.Context(), obj); err != nil {
		writeS3Error(w, 500, "InternalError", err.Error(), r.URL.Path)
		return
	}
	_ = s.repo.DeleteMultipartUpload(r.Context(), uploadID)
	_ = s.store.DeleteMultipart(r.Context(), uploadID)
	writeXML(w, http.StatusOK, completeMultipartUploadResult{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", Location: "/" + upload.Bucket + "/" + upload.Key, Bucket: upload.Bucket, Key: upload.Key, ETag: `"` + stored.ETag + `"`})
}

func (s *Service) openRequestedParts(ctx context.Context, saved []types.MultipartPart, requested []completePart) ([]io.Reader, func(), error) {
	byNumber := map[int]types.MultipartPart{}
	for _, p := range saved {
		byNumber[p.PartNumber] = p
	}
	if len(requested) == 0 {
		for _, p := range saved {
			requested = append(requested, completePart{PartNumber: p.PartNumber, ETag: p.ETag})
		}
	}
	var closers []io.Closer
	var readers []io.Reader
	closeAll := func() {
		for _, c := range closers {
			_ = c.Close()
		}
	}
	for _, req := range requested {
		part, ok := byNumber[req.PartNumber]
		if !ok {
			closeAll()
			return nil, nil, fmt.Errorf("part %d is missing", req.PartNumber)
		}
		if trimETag(req.ETag) != "" && trimETag(req.ETag) != part.ETag {
			closeAll()
			return nil, nil, fmt.Errorf("part %d etag mismatch", req.PartNumber)
		}
		reader, err := s.store.OpenMultipartPart(ctx, part.Path)
		if err != nil {
			closeAll()
			return nil, nil, err
		}
		closers = append(closers, reader)
		readers = append(readers, reader)
	}
	return readers, closeAll, nil
}

func userMetadata(r *http.Request) map[string]string {
	out := map[string]string{}
	for key, values := range r.Header {
		lower := strings.ToLower(key)
		if strings.HasPrefix(lower, "x-amz-meta-") && len(values) > 0 {
			out[strings.TrimPrefix(lower, "x-amz-meta-")] = values[0]
		}
	}
	return out
}

func writeXML(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(value)
}

func intQuery(r *http.Request, key string, fallback int) int {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return value
}

func mapRepoErr(err error) (int, string) {
	switch {
	case errors.Is(err, meta.ErrBucketExists):
		return http.StatusConflict, "BucketAlreadyOwnedByYou"
	case errors.Is(err, meta.ErrBucketNotFound):
		return http.StatusNotFound, "NoSuchBucket"
	case errors.Is(err, meta.ErrBucketNotEmpty):
		return http.StatusConflict, "BucketNotEmpty"
	case errors.Is(err, meta.ErrObjectNotFound):
		return http.StatusNotFound, "NoSuchKey"
	case errors.Is(err, meta.ErrUploadNotFound):
		return http.StatusNotFound, "NoSuchUpload"
	default:
		return http.StatusInternalServerError, "InternalError"
	}
}

func trimETag(etag string) string {
	return strings.Trim(etag, `"`)
}

var _ = bytes.MinRead
