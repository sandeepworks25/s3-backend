package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"golang.org/x/crypto/bcrypt"

	"s3store/backend/internal/config"
	"s3store/backend/internal/meta"
	"s3store/backend/internal/redisx"
	"s3store/backend/internal/storage"
	"s3store/backend/internal/types"
)

type Service struct {
	repo   meta.Repository
	store  storage.Backend
	cache  *redisx.Client
	cfg    config.Config
	logger *slog.Logger
}

func NewService(repo meta.Repository, store storage.Backend, cache *redisx.Client, cfg config.Config, logger *slog.Logger) *Service {
	return &Service{repo: repo, store: store, cache: cache, cfg: cfg, logger: logger}
}

func (s *Service) SeedRootAdmin(ctx context.Context) error {
	if strings.TrimSpace(s.cfg.RootAdminEmail) == "" || s.cfg.RootAdminPassword == "" {
		return errors.New("ROOT_ADMIN_EMAIL and ROOT_ADMIN_PASSWORD are required")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(s.cfg.RootAdminPassword), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	if err := s.repo.UpsertAdminUser(ctx, meta.AdminUser{
		Email:        strings.TrimSpace(s.cfg.RootAdminEmail),
		Name:         "Root Admin",
		PasswordHash: string(hash),
		Role:         "Admin",
		Status:       "Active",
	}); err != nil {
		return err
	}
	_ = s.repo.AddAudit(ctx, s.cfg.RootAdminEmail, "SeedAdmin", "admin", "success")
	s.logger.Info("root admin seeded", "email", s.cfg.RootAdminEmail)
	return nil
}

func (s *Service) SeedDevAccessKey(ctx context.Context) error {
	if strings.TrimSpace(s.cfg.DevAccessKey) == "" || strings.TrimSpace(s.cfg.DevSecretKey) == "" {
		return nil
	}
	return s.repo.UpsertAccessKey(ctx, meta.AccessKey{
		ID:          "dev-default",
		Label:       "Development key",
		AccessKeyID: strings.TrimSpace(s.cfg.DevAccessKey),
		SecretKey:   strings.TrimSpace(s.cfg.DevSecretKey),
		OwnerEmail:  strings.TrimSpace(s.cfg.RootAdminEmail),
		Permissions: []string{"s3:*"},
		Status:      "Active",
	})
}

func (s *Service) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(cors)
	r.Get("/healthz", s.healthz)
	r.Route("/api", func(r chi.Router) {
		r.Post("/auth/login", s.login)
		r.Post("/auth/logout", s.logout)
		r.Group(func(r chi.Router) {
			r.Use(s.requireSession)
			r.Get("/auth/me", s.me)
			r.Get("/dashboard", s.dashboard)
			r.Get("/buckets", s.listBuckets)
			r.Post("/buckets", s.createBucket)
			r.Put("/buckets/{bucket}", s.renameBucket)
			r.Delete("/buckets/{bucket}", s.deleteBucket)
			r.Get("/buckets/{bucket}/objects", s.listObjects)
			r.Post("/buckets/{bucket}/objects/upload", s.uploadObject)
			r.Get("/buckets/{bucket}/objects/download", s.downloadObject)
			r.Delete("/buckets/{bucket}/objects", s.deleteObject)
			r.Get("/streams", s.listStreams)
			r.Post("/streams", s.createStream)
			r.Get("/streams/{id}", s.getStream)
			r.Get("/streams/{id}/segments", s.listSegments)
			r.Get("/streams/{id}/playlist.m3u8", s.playlist)
			r.Get("/storage/health", s.storageHealth)
			r.Get("/usage/summary", s.usageSummary)
			r.Get("/audit-logs", s.auditLogs)
			r.Get("/settings", s.getSettings)
			r.Put("/settings", s.saveSettings)
			r.Get("/policies", s.listPolicies)
			r.Post("/policies", s.createPolicy)
			r.Put("/policies/{id}", s.updatePolicy)
			r.Delete("/policies/{id}", s.deletePolicy)
			r.Get("/users", s.listUsersAndAccessKeys)
			r.Post("/users", s.createUser)
			r.Put("/users/{email}", s.updateUser)
			r.Delete("/users/{email}", s.deleteUser)
			r.Post("/access-keys", s.createAccessKey)
			r.Put("/access-keys/{id}", s.updateAccessKey)
			r.Post("/access-keys/{id}/rotate", s.rotateAccessKey)
			r.Delete("/access-keys/{id}", s.deleteAccessKey)
			r.Get("/quotas", s.quotas)
			r.Put("/quotas", s.saveQuotas)
		})
	})
	r.Head("/{bucket}/*", s.publicObject)
	r.Get("/{bucket}/*", s.publicObject)
	return r
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Range")
			w.Header().Set("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
			w.Header().Set("Access-Control-Expose-Headers", "Accept-Ranges, Content-Range, Content-Length, Content-Type, ETag")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Service) login(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	req.Email = strings.TrimSpace(req.Email)
	user, err := s.repo.GetAdminUserByEmail(r.Context(), req.Email)
	if err != nil || bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)) != nil {
		writeJSONError(w, http.StatusUnauthorized, "invalid email or password")
		return
	}
	if user.Status == "Disabled" {
		writeJSONError(w, http.StatusForbidden, "user is disabled")
		return
	}
	settings, _ := s.repo.GetSystemSettings(r.Context())
	sessionTTL := time.Duration(settings.SessionTTLHours) * time.Hour
	if sessionTTL <= 0 {
		sessionTTL = 24 * time.Hour
	}
	sessionID := randomSessionID()
	s.cache.Set(r.Context(), "session:"+sessionID, user.Email, sessionTTL)
	http.SetCookie(w, &http.Cookie{Name: "s3store_session", Value: sessionID, Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode, MaxAge: int(sessionTTL.Seconds())})
	_ = s.repo.AddAudit(r.Context(), user.Email, "Login", "admin", "success")
	writeJSON(w, http.StatusOK, map[string]any{"email": user.Email, "role": user.Role})
}

func (s *Service) logout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie("s3store_session"); err == nil {
		s.cache.Del(r.Context(), "session:"+c.Value)
	}
	http.SetCookie(w, &http.Cookie{Name: "s3store_session", Value: "", Path: "/", HttpOnly: true, MaxAge: -1})
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) requireSession(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := r.Cookie("s3store_session")
		if err != nil || c.Value == "" {
			writeJSONError(w, http.StatusUnauthorized, "login required")
			return
		}
		if _, ok := s.cache.Get(r.Context(), "session:"+c.Value); !ok {
			writeJSONError(w, http.StatusUnauthorized, "session expired")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Service) me(w http.ResponseWriter, r *http.Request) {
	c, _ := r.Cookie("s3store_session")
	email, _ := s.cache.Get(r.Context(), "session:"+c.Value)
	user, err := s.repo.GetAdminUserByEmail(r.Context(), email)
	if err != nil {
		writeJSONError(w, http.StatusUnauthorized, "session user not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"email": user.Email, "role": user.Role})
}

func (s *Service) dashboard(w http.ResponseWriter, r *http.Request) {
	usage, _ := s.repo.UsageSummary(r.Context())
	health := s.healthSnapshot(r)
	audit, _ := s.repo.ListAudit(r.Context(), 8)
	writeJSON(w, http.StatusOK, map[string]any{"usage": usage, "health": health, "audit": audit})
}

func (s *Service) getSettings(w http.ResponseWriter, r *http.Request) {
	settings, err := s.repo.GetSystemSettings(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, s.settingsWithRuntime(settings))
}

func (s *Service) saveSettings(w http.ResponseWriter, r *http.Request) {
	var settings meta.SystemSettings
	if err := json.NewDecoder(r.Body).Decode(&settings); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if err := validateSystemSettings(settings); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	settings.ObjectDataRoot = ""
	settings.MultipartDataRoot = ""
	settings.NASDataRoot = ""
	settings.NASConfigured = false
	saved, err := s.repo.SaveSystemSettings(r.Context(), settings)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = s.repo.AddAudit(r.Context(), sessionEmail(r, s.cache), "UpdateSettings", "system", "success")
	writeJSON(w, http.StatusOK, s.settingsWithRuntime(saved))
}

func (s *Service) settingsWithRuntime(settings meta.SystemSettings) meta.SystemSettings {
	settings.ObjectDataRoot = s.cfg.ObjectDataRoot
	settings.MultipartDataRoot = s.cfg.MultipartDataRoot
	settings.NASDataRoot = s.cfg.NASDataRoot
	settings.NASConfigured = strings.TrimSpace(s.cfg.NASDataRoot) != ""
	return settings
}

func (s *Service) listPolicies(w http.ResponseWriter, r *http.Request) {
	policies, err := s.repo.ListPolicies(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, policies)
}

func (s *Service) createPolicy(w http.ResponseWriter, r *http.Request) {
	var req policyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	policy, err := req.toPolicy("")
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	created, err := s.repo.CreatePolicy(r.Context(), policy)
	if err != nil {
		writeJSONError(w, mapStatus(err), err.Error())
		return
	}
	_ = s.repo.AddAudit(r.Context(), sessionEmail(r, s.cache), "CreatePolicy", created.Name, "success")
	writeJSON(w, http.StatusCreated, created)
}

func (s *Service) updatePolicy(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var req policyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	policy, err := req.toPolicy(id)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	updated, err := s.repo.UpdatePolicy(r.Context(), policy)
	if err != nil {
		writeJSONError(w, mapStatus(err), err.Error())
		return
	}
	_ = s.repo.AddAudit(r.Context(), sessionEmail(r, s.cache), "UpdatePolicy", updated.Name, "success")
	writeJSON(w, http.StatusOK, updated)
}

func (s *Service) deletePolicy(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := s.repo.DeletePolicy(r.Context(), id); err != nil {
		writeJSONError(w, mapStatus(err), err.Error())
		return
	}
	_ = s.repo.AddAudit(r.Context(), sessionEmail(r, s.cache), "DeletePolicy", id, "success")
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) listUsersAndAccessKeys(w http.ResponseWriter, r *http.Request) {
	users, err := s.repo.ListAdminUsers(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	keys, err := s.repo.ListAccessKeys(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	userResponses := make([]adminUserResponse, 0, len(users))
	for _, user := range users {
		userResponses = append(userResponses, toAdminUserResponse(user))
	}
	keyResponses := make([]accessKeyResponse, 0, len(keys))
	for _, key := range keys {
		keyResponses = append(keyResponses, toAccessKeyResponse(key, ""))
	}
	writeJSON(w, http.StatusOK, map[string]any{"users": userResponses, "keys": keyResponses})
}

func (s *Service) createUser(w http.ResponseWriter, r *http.Request) {
	var req userRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if err := req.validate(true); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	user := meta.AdminUser{
		Name:         strings.TrimSpace(req.Name),
		Email:        strings.TrimSpace(req.Email),
		PasswordHash: string(hash),
		Role:         normalizeRole(req.Role),
		Status:       normalizeStatus(req.Status),
	}
	if err := s.repo.UpsertAdminUser(r.Context(), user); err != nil {
		writeJSONError(w, mapStatus(err), err.Error())
		return
	}
	_ = s.repo.AddAudit(r.Context(), sessionEmail(r, s.cache), "CreateUser", user.Email, "success")
	created, _ := s.repo.GetAdminUserByEmail(r.Context(), user.Email)
	writeJSON(w, http.StatusCreated, toAdminUserResponse(created))
}

func (s *Service) updateUser(w http.ResponseWriter, r *http.Request) {
	email, err := url.PathUnescape(chi.URLParam(r, "email"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid email")
		return
	}
	existing, err := s.repo.GetAdminUserByEmail(r.Context(), email)
	if err != nil {
		writeJSONError(w, mapStatus(err), err.Error())
		return
	}
	var req userRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if err := req.validate(false); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	existing.Name = strings.TrimSpace(req.Name)
	existing.Role = normalizeRole(req.Role)
	existing.Status = normalizeStatus(req.Status)
	if strings.TrimSpace(req.Password) != "" {
		hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		existing.PasswordHash = string(hash)
	}
	if err := s.repo.UpsertAdminUser(r.Context(), existing); err != nil {
		writeJSONError(w, mapStatus(err), err.Error())
		return
	}
	_ = s.repo.AddAudit(r.Context(), sessionEmail(r, s.cache), "UpdateUser", existing.Email, "success")
	updated, _ := s.repo.GetAdminUserByEmail(r.Context(), existing.Email)
	writeJSON(w, http.StatusOK, toAdminUserResponse(updated))
}

func (s *Service) deleteUser(w http.ResponseWriter, r *http.Request) {
	email, err := url.PathUnescape(chi.URLParam(r, "email"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid email")
		return
	}
	if err := s.repo.DeleteAdminUser(r.Context(), email); err != nil {
		writeJSONError(w, mapStatus(err), err.Error())
		return
	}
	_ = s.repo.AddAudit(r.Context(), sessionEmail(r, s.cache), "DeleteUser", email, "success")
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) createAccessKey(w http.ResponseWriter, r *http.Request) {
	var req accessKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if err := req.validate(); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	if _, err := s.repo.GetAdminUserByEmail(r.Context(), strings.TrimSpace(req.OwnerEmail)); err != nil {
		writeJSONError(w, mapStatus(err), err.Error())
		return
	}
	secret := randomSecretKey()
	key := meta.AccessKey{
		Label:       strings.TrimSpace(req.Label),
		AccessKeyID: randomAccessKeyID(),
		SecretKey:   secret,
		OwnerEmail:  strings.TrimSpace(req.OwnerEmail),
		Permissions: normalizePermissions(req.Permissions),
		Status:      normalizeStatus(req.Status),
	}
	if err := s.repo.UpsertAccessKey(r.Context(), key); err != nil {
		writeJSONError(w, mapStatus(err), err.Error())
		return
	}
	_ = s.repo.AddAudit(r.Context(), sessionEmail(r, s.cache), "CreateAccessKey", key.Label, "success")
	keys, _ := s.repo.ListAccessKeys(r.Context())
	for _, created := range keys {
		if created.AccessKeyID == key.AccessKeyID {
			writeJSON(w, http.StatusCreated, toAccessKeyResponse(created, secret))
			return
		}
	}
	writeJSON(w, http.StatusCreated, toAccessKeyResponse(key, secret))
}

func (s *Service) updateAccessKey(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	key, err := s.repo.GetAccessKey(r.Context(), id)
	if err != nil {
		writeJSONError(w, mapStatus(err), err.Error())
		return
	}
	var req accessKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if err := req.validate(); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	if _, err := s.repo.GetAdminUserByEmail(r.Context(), strings.TrimSpace(req.OwnerEmail)); err != nil {
		writeJSONError(w, mapStatus(err), err.Error())
		return
	}
	key.Label = strings.TrimSpace(req.Label)
	key.OwnerEmail = strings.TrimSpace(req.OwnerEmail)
	key.Permissions = normalizePermissions(req.Permissions)
	key.Status = normalizeStatus(req.Status)
	if err := s.repo.UpsertAccessKey(r.Context(), key); err != nil {
		writeJSONError(w, mapStatus(err), err.Error())
		return
	}
	_ = s.repo.AddAudit(r.Context(), sessionEmail(r, s.cache), "UpdateAccessKey", key.Label, "success")
	updated, _ := s.repo.GetAccessKey(r.Context(), id)
	writeJSON(w, http.StatusOK, toAccessKeyResponse(updated, ""))
}

func (s *Service) rotateAccessKey(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	key, err := s.repo.GetAccessKey(r.Context(), id)
	if err != nil {
		writeJSONError(w, mapStatus(err), err.Error())
		return
	}
	secret := randomSecretKey()
	key.AccessKeyID = randomAccessKeyID()
	key.SecretKey = secret
	key.Status = "Active"
	if err := s.repo.UpsertAccessKey(r.Context(), key); err != nil {
		writeJSONError(w, mapStatus(err), err.Error())
		return
	}
	_ = s.repo.AddAudit(r.Context(), sessionEmail(r, s.cache), "RotateAccessKey", key.Label, "success")
	updated, _ := s.repo.GetAccessKey(r.Context(), id)
	writeJSON(w, http.StatusOK, toAccessKeyResponse(updated, secret))
}

func (s *Service) deleteAccessKey(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := s.repo.DeleteAccessKey(r.Context(), id); err != nil {
		writeJSONError(w, mapStatus(err), err.Error())
		return
	}
	_ = s.repo.AddAudit(r.Context(), sessionEmail(r, s.cache), "DeleteAccessKey", id, "success")
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) listBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := s.repo.ListBuckets(r.Context())
	if err != nil {
		writeJSONError(w, 500, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, buckets)
}

func (s *Service) createBucket(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name   string `json:"name"`
		Policy string `json:"policy"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		writeJSONError(w, http.StatusBadRequest, "bucket name is required")
		return
	}
	b, err := s.repo.CreateBucket(r.Context(), strings.TrimSpace(req.Name), normalizeBucketPolicy(req.Policy))
	if err != nil {
		writeJSONError(w, mapStatus(err), err.Error())
		return
	}
	_ = s.repo.AddAudit(r.Context(), "admin", "CreateBucket", req.Name, "success")
	writeJSON(w, http.StatusCreated, b)
}

func (s *Service) renameBucket(w http.ResponseWriter, r *http.Request) {
	oldName := chi.URLParam(r, "bucket")
	var req struct {
		Name   string `json:"name"`
		Policy string `json:"policy"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.Name) == "" {
		writeJSONError(w, http.StatusBadRequest, "new bucket name is required")
		return
	}
	newName := strings.TrimSpace(req.Name)
	if oldName != newName {
		if err := s.store.RenameBucket(r.Context(), oldName, newName); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	b, err := s.repo.UpdateBucket(r.Context(), oldName, newName, normalizeBucketPolicy(req.Policy))
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = s.repo.AddAudit(r.Context(), "admin", "UpdateBucket", oldName+" -> "+newName, "success")
	writeJSON(w, http.StatusOK, b)
}

func (s *Service) deleteBucket(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	if r.URL.Query().Get("force") == "1" || r.URL.Query().Get("recursive") == "1" {
		if err := s.store.DeleteBucket(r.Context(), bucket); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if err := s.repo.DeleteBucketRecursive(r.Context(), bucket); err != nil {
			writeJSONError(w, mapStatus(err), err.Error())
			return
		}
		_ = s.repo.AddAudit(r.Context(), "admin", "DeleteBucketRecursive", bucket, "success")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err := s.repo.DeleteBucket(r.Context(), bucket); err != nil {
		writeJSONError(w, mapStatus(err), err.Error())
		return
	}
	_ = s.store.DeleteBucket(r.Context(), bucket)
	_ = s.repo.AddAudit(r.Context(), "admin", "DeleteBucket", bucket, "success")
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) listObjects(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	prefix := r.URL.Query().Get("prefix")
	cursor := r.URL.Query().Get("cursor")
	limit := intQuery(r, "limit", 100)
	objects, next, err := s.repo.ListObjects(r.Context(), bucket, prefix, limit, cursor)
	if err != nil {
		writeJSONError(w, mapStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": objects, "nextCursor": next})
}

func (s *Service) uploadObject(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	key := r.URL.Query().Get("key")
	if key == "" {
		writeJSONError(w, http.StatusBadRequest, "key is required")
		return
	}
	settings, _ := s.repo.GetSystemSettings(r.Context())
	maxBytes := int64(settings.MaxUploadMB) * 1024 * 1024
	if maxBytes > 0 {
		if r.ContentLength > maxBytes {
			writeJSONError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("upload exceeds max size of %d MB", settings.MaxUploadMB))
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	}
	stored, err := s.store.Put(r.Context(), storage.ObjectWrite{Bucket: bucket, Key: key, ContentType: r.Header.Get("Content-Type"), Body: r.Body})
	if err != nil {
		if strings.Contains(err.Error(), "http: request body too large") {
			writeJSONError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("upload exceeds max size of %d MB", settings.MaxUploadMB))
			return
		}
		writeJSONError(w, 500, err.Error())
		return
	}
	obj := types.Object{Bucket: bucket, Key: key, Size: stored.Size, ETag: stored.ETag, ContentType: stored.ContentType, StorageBackend: stored.StorageBackend, PhysicalPath: stored.PhysicalPath}
	if err := s.repo.UpsertObject(r.Context(), obj); err != nil {
		writeJSONError(w, mapStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, obj)
}

func (s *Service) downloadObject(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	key := r.URL.Query().Get("key")
	obj, err := s.repo.GetObject(r.Context(), bucket, key)
	if err != nil {
		writeJSONError(w, mapStatus(err), err.Error())
		return
	}
	reader, _, err := s.store.Get(r.Context(), bucket, key, nil)
	if err != nil {
		writeJSONError(w, 404, err.Error())
		return
	}
	defer reader.Close()
	w.Header().Set("Content-Type", obj.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	disposition := "attachment"
	if r.URL.Query().Get("inline") == "1" {
		disposition = "inline"
	}
	w.Header().Set("Content-Disposition", disposition+`; filename="`+safeFilename(key)+`"`)
	_, _ = io.Copy(w, reader)
}

func (s *Service) publicObject(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	key, obj, err := s.publicObjectLookup(r, bucket)
	if err != nil {
		writeJSONError(w, mapStatus(err), err.Error())
		return
	}
	if servePhysicalObject(w, r, obj, key, `inline; filename="`+safeFilename(key)+`"`, true) {
		return
	}
	rangeSpec, partial := storage.ParseRange(r.Header.Get("Range"), obj.Size)
	reader, _, err := s.store.Get(r.Context(), bucket, key, rangeSpec)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, err.Error())
		return
	}
	defer reader.Close()
	w.Header().Set("Content-Type", publicContentType(obj))
	w.Header().Set("ETag", `"`+obj.ETag+`"`)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Cache-Control", "public, max-age=3600")
	w.Header().Set("Content-Disposition", `inline; filename="`+safeFilename(key)+`"`)
	if partial {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rangeSpec.Start, rangeSpec.End, obj.Size))
		w.Header().Set("Content-Length", strconv.FormatInt(rangeSpec.End-rangeSpec.Start+1, 10))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	}
	if r.Method == http.MethodHead {
		return
	}
	_, _ = io.Copy(w, reader)
}

func servePhysicalObject(w http.ResponseWriter, r *http.Request, obj types.Object, filename, disposition string, cache bool) bool {
	if obj.PhysicalPath == "" {
		return false
	}
	file, err := os.Open(obj.PhysicalPath)
	if err != nil {
		return false
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.IsDir() {
		return false
	}
	w.Header().Set("Content-Type", publicContentType(obj))
	w.Header().Set("ETag", `"`+obj.ETag+`"`)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Disposition", disposition)
	if cache {
		w.Header().Set("Cache-Control", "public, max-age=3600")
	}
	http.ServeContent(w, r, filename, info.ModTime(), file)
	return true
}

func (s *Service) publicObjectLookup(r *http.Request, bucket string) (string, types.Object, error) {
	candidates := []string{chi.URLParam(r, "*")}
	escapedPrefix := "/" + url.PathEscape(bucket) + "/"
	plainPrefix := "/" + bucket + "/"
	for _, prefix := range []string{escapedPrefix, plainPrefix} {
		if raw, ok := strings.CutPrefix(r.URL.EscapedPath(), prefix); ok {
			candidates = append(candidates, raw)
		}
	}
	var lastErr error
	seen := map[string]bool{}
	for _, candidate := range candidates {
		for _, key := range publicObjectKeyVariants(candidate) {
			if key == "" || seen[key] {
				continue
			}
			seen[key] = true
			obj, err := s.repo.GetObject(r.Context(), bucket, key)
			if err == nil {
				return key, obj, nil
			}
			lastErr = err
		}
	}
	if key, obj, ok := s.findPublicObjectByNormalizedKey(r, bucket, candidates); ok {
		return key, obj, nil
	}
	if lastErr == nil {
		lastErr = meta.ErrObjectNotFound
	}
	return candidates[0], types.Object{}, lastErr
}

func publicObjectKeyVariants(key string) []string {
	variants := []string{key}
	if decoded, err := url.PathUnescape(key); err == nil {
		variants = append(variants, decoded)
	}
	if decoded, err := url.QueryUnescape(key); err == nil {
		variants = append(variants, decoded)
	}
	variants = append(variants, strings.ReplaceAll(key, "+", " "))
	return variants
}

func (s *Service) findPublicObjectByNormalizedKey(r *http.Request, bucket string, candidates []string) (string, types.Object, bool) {
	targets := map[string]bool{}
	for _, candidate := range candidates {
		for _, variant := range publicObjectKeyVariants(candidate) {
			targets[normalizeObjectKeyForLookup(variant)] = true
		}
	}
	objects, _, err := s.repo.ListObjects(r.Context(), bucket, "", 1000, "")
	if err != nil {
		return "", types.Object{}, false
	}
	for _, obj := range objects {
		if targets[normalizeObjectKeyForLookup(obj.Key)] {
			return obj.Key, obj, true
		}
	}
	return "", types.Object{}, false
}

func normalizeObjectKeyForLookup(key string) string {
	key = strings.TrimSpace(key)
	key = strings.ReplaceAll(key, "+", " ")
	return strings.Join(strings.Fields(key), " ")
}

func publicContentType(obj types.Object) string {
	contentType := obj.ContentType
	if contentType == "" || contentType == "application/octet-stream" {
		lower := strings.ToLower(obj.Key)
		switch {
		case strings.HasSuffix(lower, ".pdf"):
			return "application/pdf"
		case strings.HasSuffix(lower, ".mp4"):
			return "video/mp4"
		case strings.HasSuffix(lower, ".webm"):
			return "video/webm"
		case strings.HasSuffix(lower, ".png"):
			return "image/png"
		case strings.HasSuffix(lower, ".jpg"), strings.HasSuffix(lower, ".jpeg"):
			return "image/jpeg"
		case strings.HasSuffix(lower, ".txt"):
			return "text/plain; charset=utf-8"
		case strings.HasSuffix(lower, ".json"):
			return "application/json"
		}
	}
	if contentType == "" {
		return "application/octet-stream"
	}
	return contentType
}

func (s *Service) deleteObject(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	key := r.URL.Query().Get("key")
	_ = s.store.Delete(r.Context(), bucket, key)
	if err := s.repo.DeleteObject(r.Context(), bucket, key); err != nil {
		writeJSONError(w, mapStatus(err), err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) listStreams(w http.ResponseWriter, r *http.Request) {
	streams, err := s.repo.ListStreams(r.Context())
	if err != nil {
		writeJSONError(w, 500, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, streams)
}

func (s *Service) createStream(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name   string `json:"name"`
		Bucket string `json:"bucket"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" || req.Bucket == "" {
		writeJSONError(w, http.StatusBadRequest, "name and bucket are required")
		return
	}
	stream := types.Stream{ID: "stream-" + randomSessionID()[:12], Name: req.Name, Bucket: req.Bucket, Status: "active", CreatedAt: time.Now().UTC()}
	if err := s.repo.CreateStream(r.Context(), stream); err != nil {
		writeJSONError(w, mapStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, stream)
}

func (s *Service) getStream(w http.ResponseWriter, r *http.Request) {
	stream, err := s.repo.GetStream(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeJSONError(w, mapStatus(err), err.Error())
		return
	}
	segments, _ := s.repo.ListStreamSegments(r.Context(), stream.ID)
	writeJSON(w, http.StatusOK, map[string]any{"stream": stream, "segments": segments})
}

func (s *Service) listSegments(w http.ResponseWriter, r *http.Request) {
	segments, err := s.repo.ListStreamSegments(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeJSONError(w, 500, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, segments)
}

func (s *Service) playlist(w http.ResponseWriter, r *http.Request) {
	stream, err := s.repo.GetStream(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeJSONError(w, mapStatus(err), err.Error())
		return
	}
	segments, _ := s.repo.ListStreamSegments(r.Context(), stream.ID)
	var b strings.Builder
	b.WriteString("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:4\n#EXT-X-MEDIA-SEQUENCE:0\n")
	for _, seg := range segments {
		duration := float64(seg.DurationMS) / 1000
		if duration <= 0 {
			duration = 4
		}
		b.WriteString(fmt.Sprintf("#EXTINF:%.3f,\n/%s/%s\n", duration, stream.Bucket, seg.ObjectKey))
	}
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	_, _ = w.Write([]byte(b.String()))
}

func (s *Service) storageHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.healthSnapshot(r))
}

func (s *Service) usageSummary(w http.ResponseWriter, r *http.Request) {
	usage, err := s.repo.UsageSummary(r.Context())
	if err != nil {
		writeJSONError(w, 500, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, usage)
}

func (s *Service) auditLogs(w http.ResponseWriter, r *http.Request) {
	items, err := s.repo.ListAudit(r.Context(), intQuery(r, "limit", 100))
	if err != nil {
		writeJSONError(w, 500, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (s *Service) quotas(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"enabled": false, "message": "operator quotas are disabled by default"})
}

func (s *Service) saveQuotas(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusAccepted, map[string]any{"enabled": false, "message": "quota persistence is intentionally disabled in this MVP"})
}

func (s *Service) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.healthSnapshot(r))
}

func (s *Service) healthSnapshot(r *http.Request) map[string]any {
	ctx := r.Context()
	dbErr := s.repo.Health(ctx)
	redisErr := s.cache.Health(ctx)
	return map[string]any{
		"api":      map[string]any{"healthy": true, "message": "ok"},
		"postgres": healthItem(dbErr),
		"redis":    healthItem(redisErr),
		"storage":  s.store.Health(ctx),
	}
}

func healthItem(err error) map[string]any {
	if err != nil {
		return map[string]any{"healthy": false, "message": err.Error()}
	}
	return map[string]any{"healthy": true, "message": "ok"}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func validateSystemSettings(settings meta.SystemSettings) error {
	if strings.TrimSpace(settings.OrganizationName) == "" {
		return errors.New("organization name is required")
	}
	if strings.TrimSpace(settings.PublicBaseURL) != "" {
		u, err := url.Parse(settings.PublicBaseURL)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return errors.New("public base URL must be a valid absolute URL")
		}
	}
	if settings.SessionTTLHours <= 0 || settings.SessionTTLHours > 720 {
		return errors.New("session TTL must be between 1 and 720 hours")
	}
	if settings.MaxUploadMB <= 0 || settings.MaxUploadMB > 1024*1024 {
		return errors.New("max upload must be between 1 MB and 1 TB")
	}
	return nil
}

type policyRequest struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Effect      string         `json:"effect"`
	Actions     []string       `json:"actions"`
	Resource    string         `json:"resource"`
	Document    map[string]any `json:"document"`
}

func (r policyRequest) toPolicy(id string) (meta.Policy, error) {
	name := strings.TrimSpace(r.Name)
	if name == "" {
		return meta.Policy{}, errors.New("name is required")
	}
	resource := strings.TrimSpace(r.Resource)
	if resource == "" {
		return meta.Policy{}, errors.New("resource is required")
	}
	actions := normalizePermissions(r.Actions)
	effect := "Allow"
	if strings.EqualFold(strings.TrimSpace(r.Effect), "deny") {
		effect = "Deny"
	}
	document := r.Document
	if document == nil {
		document = policyDocument(effect, actions, resource)
	}
	return meta.Policy{
		ID:          id,
		Name:        name,
		Description: strings.TrimSpace(r.Description),
		Effect:      effect,
		Actions:     actions,
		Resource:    resource,
		Document:    document,
	}, nil
}

func policyDocument(effect string, actions []string, resource string) map[string]any {
	return map[string]any{
		"Version": "2026-06-22",
		"Statement": []map[string]any{
			{
				"Effect":   effect,
				"Action":   actions,
				"Resource": resource,
			},
		},
	}
}

type userRequest struct {
	Name     string `json:"name"`
	Email    string `json:"email"`
	Password string `json:"password"`
	Role     string `json:"role"`
	Status   string `json:"status"`
}

func (r userRequest) validate(requirePassword bool) error {
	if strings.TrimSpace(r.Name) == "" {
		return errors.New("name is required")
	}
	if strings.TrimSpace(r.Email) == "" && requirePassword {
		return errors.New("email is required")
	}
	if requirePassword && strings.TrimSpace(r.Password) == "" {
		return errors.New("password is required")
	}
	return nil
}

type accessKeyRequest struct {
	Label       string   `json:"label"`
	OwnerEmail  string   `json:"ownerEmail"`
	Permissions []string `json:"permissions"`
	Status      string   `json:"status"`
}

func (r accessKeyRequest) validate() error {
	if strings.TrimSpace(r.Label) == "" {
		return errors.New("label is required")
	}
	if strings.TrimSpace(r.OwnerEmail) == "" {
		return errors.New("owner is required")
	}
	return nil
}

type adminUserResponse struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Email     string `json:"email"`
	Role      string `json:"role"`
	Status    string `json:"status"`
	LastSeen  string `json:"lastSeen"`
	CreatedAt string `json:"createdAt"`
	UpdatedAt string `json:"updatedAt"`
}

type accessKeyResponse struct {
	ID          string   `json:"id"`
	Label       string   `json:"label"`
	AccessKeyID string   `json:"accessKeyId"`
	Prefix      string   `json:"prefix"`
	SecretKey   string   `json:"secretKey,omitempty"`
	OwnerEmail  string   `json:"ownerEmail"`
	Permissions []string `json:"permissions"`
	Status      string   `json:"status"`
	LastUsed    string   `json:"lastUsed"`
	CreatedAt   string   `json:"createdAt"`
	UpdatedAt   string   `json:"updatedAt"`
}

func toAdminUserResponse(user meta.AdminUser) adminUserResponse {
	return adminUserResponse{
		ID:        user.ID,
		Name:      user.Name,
		Email:     user.Email,
		Role:      normalizeRole(user.Role),
		Status:    normalizeStatus(user.Status),
		LastSeen:  displayTime(user.LastSeen),
		CreatedAt: user.CreatedAt,
		UpdatedAt: user.UpdatedAt,
	}
}

func toAccessKeyResponse(key meta.AccessKey, secret string) accessKeyResponse {
	return accessKeyResponse{
		ID:          key.ID,
		Label:       key.Label,
		AccessKeyID: key.AccessKeyID,
		Prefix:      accessKeyPrefix(key.AccessKeyID),
		SecretKey:   secret,
		OwnerEmail:  key.OwnerEmail,
		Permissions: normalizePermissions(key.Permissions),
		Status:      normalizeStatus(key.Status),
		LastUsed:    displayTime(key.LastUsed),
		CreatedAt:   key.CreatedAt,
		UpdatedAt:   key.UpdatedAt,
	}
}

func normalizeRole(role string) string {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case "operator":
		return "Operator"
	case "viewer":
		return "Viewer"
	case "auditor":
		return "Auditor"
	default:
		return "Admin"
	}
}

func normalizeStatus(status string) string {
	if strings.EqualFold(strings.TrimSpace(status), "disabled") {
		return "Disabled"
	}
	return "Active"
}

func normalizePermissions(permissions []string) []string {
	if len(permissions) == 0 {
		return []string{"s3:*"}
	}
	out := make([]string, 0, len(permissions))
	seen := map[string]bool{}
	for _, permission := range permissions {
		permission = strings.TrimSpace(permission)
		if permission == "" || seen[permission] {
			continue
		}
		seen[permission] = true
		out = append(out, permission)
	}
	if len(out) == 0 {
		return []string{"s3:*"}
	}
	return out
}

func accessKeyPrefix(accessKeyID string) string {
	if len(accessKeyID) <= 8 {
		return accessKeyID
	}
	return accessKeyID[:4] + "..." + accessKeyID[len(accessKeyID)-4:]
}

func displayTime(value string) string {
	if strings.TrimSpace(value) == "" {
		return "Never"
	}
	return value
}

func sessionEmail(r *http.Request, cache *redisx.Client) string {
	c, err := r.Cookie("s3store_session")
	if err != nil {
		return "system"
	}
	email, ok := cache.Get(r.Context(), "session:"+c.Value)
	if !ok || email == "" {
		return "system"
	}
	return email
}

func mapStatus(err error) int {
	switch {
	case errors.Is(err, meta.ErrBucketNotFound), errors.Is(err, meta.ErrObjectNotFound), errors.Is(err, meta.ErrStreamNotFound), errors.Is(err, meta.ErrAdminNotFound), errors.Is(err, meta.ErrAccessKeyNotFound):
		return http.StatusNotFound
	case errors.Is(err, meta.ErrBucketExists), errors.Is(err, meta.ErrBucketNotEmpty):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}

func intQuery(r *http.Request, key string, fallback int) int {
	value, err := strconv.Atoi(r.URL.Query().Get(key))
	if err != nil {
		return fallback
	}
	return value
}

func safeFilename(key string) string {
	parts := strings.Split(key, "/")
	return parts[len(parts)-1]
}

func normalizeBucketPolicy(policy string) string {
	switch strings.ToLower(strings.TrimSpace(policy)) {
	case "public":
		return "public"
	case "read-only", "readonly":
		return "read-only"
	default:
		return "private"
	}
}
