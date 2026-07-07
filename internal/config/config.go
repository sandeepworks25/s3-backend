package config

import (
	"bufio"
	"os"
	"strings"
)

type Config struct {
	S3APIAddr         string
	AdminAPIAddr      string
	DatabaseURL       string
	RedisAddr         string
	ObjectDataRoot    string
	MultipartDataRoot string
	NASDataRoot       string
	RootAdminEmail    string
	RootAdminPassword string
	SessionSecret     string
	DevAccessKey      string
	DevSecretKey      string
}

func Load() Config {
	loadDotEnv(".env")

	return Config{
		S3APIAddr:         env("S3_API_ADDR", ":9000"),
		AdminAPIAddr:      env("ADMIN_API_ADDR", ":9001"),
		DatabaseURL:       env("DATABASE_URL", ""),
		RedisAddr:         env("REDIS_ADDR", "localhost:6379"),
		ObjectDataRoot:    env("OBJECT_DATA_ROOT", "./data/objects"),
		MultipartDataRoot: env("MULTIPART_DATA_ROOT", "./data/multipart"),
		NASDataRoot:       env("NAS_DATA_ROOT", ""),
		RootAdminEmail:    env("ROOT_ADMIN_EMAIL", "admin@example.com"),
		RootAdminPassword: env("ROOT_ADMIN_PASSWORD", "change-me-now"),
		SessionSecret:     env("SESSION_SECRET", "dev-session-secret"),
		DevAccessKey:      env("DEV_ACCESS_KEY", "dev-access-key"),
		DevSecretKey:      env("DEV_SECRET_KEY", "dev-secret-key"),
	}
}

func loadDotEnv(path string) {
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		if key == "" || os.Getenv(key) != "" {
			continue
		}
		_ = os.Setenv(key, value)
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
