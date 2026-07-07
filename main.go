package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"s3store/backend/internal/admin"
	"s3store/backend/internal/auth"
	"s3store/backend/internal/config"
	"s3store/backend/internal/meta"
	"s3store/backend/internal/redisx"
	"s3store/backend/internal/s3api"
	"s3store/backend/internal/storage"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := config.Load()

	repo, err := meta.OpenPostgres(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		logger.Warn("postgres unavailable, using in-memory metadata repository", "error", err)
		repo = meta.NewMemoryRepository()
	}
	defer repo.Close()

	cache := redisx.Open(cfg.RedisAddr, logger)
	defer cache.Close()

	store, err := storage.NewHybridBackend(storage.HybridConfig{
		LocalRoot:     cfg.ObjectDataRoot,
		MultipartRoot: cfg.MultipartDataRoot,
		NASRoot:       cfg.NASDataRoot,
	})
	if err != nil {
		logger.Error("storage backend failed", "error", err)
		os.Exit(1)
	}

	adminsvc := admin.NewService(repo, store, cache, cfg, logger)
	if err := adminsvc.SeedRootAdmin(ctx); err != nil {
		logger.Error("root admin seed failed", "error", err)
		os.Exit(1)
	}
	if err := adminsvc.SeedDevAccessKey(ctx); err != nil {
		logger.Error("dev access key seed failed", "error", err)
		os.Exit(1)
	}

	keys := auth.NewLookupKeyStore(func(ctx context.Context, accessKeyID string) (auth.AccessKey, bool) {
		key, err := repo.GetAccessKeyByAccessKeyID(ctx, accessKeyID)
		if err != nil || key.Status != "Active" {
			return auth.AccessKey{}, false
		}
		_ = repo.TouchAccessKey(ctx, accessKeyID)
		return auth.AccessKey{AccessKeyID: key.AccessKeyID, SecretKey: key.SecretKey, Enabled: true}, true
	})
	s3svc := s3api.NewService(repo, store, keys, cache, logger)

	s3Server := &http.Server{
		Addr:              cfg.S3APIAddr,
		Handler:           s3svc.Router(),
		ReadHeaderTimeout: 15 * time.Second,
	}
	adminServer := &http.Server{
		Addr:              cfg.AdminAPIAddr,
		Handler:           adminsvc.Router(),
		ReadHeaderTimeout: 15 * time.Second,
	}

	go serve(logger, "s3-api", s3Server)
	go serve(logger, "admin-api", adminServer)

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = s3Server.Shutdown(shutdownCtx)
	_ = adminServer.Shutdown(shutdownCtx)
	logger.Info("shutdown complete")
}

func serve(logger *slog.Logger, name string, srv *http.Server) {
	logger.Info("server starting", "name", name, "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server stopped unexpectedly", "name", name, "error", err)
		os.Exit(1)
	}
}
