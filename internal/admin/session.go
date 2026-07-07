package admin

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
)

func randomSessionID() string {
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "session-fallback"
	}
	return hex.EncodeToString(b[:])
}

func randomAccessKeyID() string {
	return "S3" + strings.ToUpper(randomHex(14))
}

func randomSecretKey() string {
	return randomHex(32)
}

func randomHex(size int) string {
	b := make([]byte, size)
	if _, err := rand.Read(b); err != nil {
		return "random-fallback"
	}
	return hex.EncodeToString(b)
}
