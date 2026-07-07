package auth

import (
	"context"
	"crypto/subtle"
)

type AccessKey struct {
	AccessKeyID string
	SecretKey   string
	Enabled     bool
}

type KeyStore interface {
	FindAccessKey(ctx context.Context, accessKeyID string) (AccessKey, bool)
}

type StaticKeyStore struct {
	accessKey string
	secretKey string
}

type LookupKeyStore struct {
	lookup func(context.Context, string) (AccessKey, bool)
}

func NewStaticKeyStore(accessKey, secretKey string) *StaticKeyStore {
	return &StaticKeyStore{accessKey: accessKey, secretKey: secretKey}
}

func (s *StaticKeyStore) FindAccessKey(_ context.Context, accessKeyID string) (AccessKey, bool) {
	if subtle.ConstantTimeCompare([]byte(accessKeyID), []byte(s.accessKey)) != 1 {
		return AccessKey{}, false
	}
	return AccessKey{AccessKeyID: s.accessKey, SecretKey: s.secretKey, Enabled: true}, true
}

func NewLookupKeyStore(lookup func(context.Context, string) (AccessKey, bool)) *LookupKeyStore {
	return &LookupKeyStore{lookup: lookup}
}

func (s *LookupKeyStore) FindAccessKey(ctx context.Context, accessKeyID string) (AccessKey, bool) {
	if s.lookup == nil {
		return AccessKey{}, false
	}
	return s.lookup(ctx, accessKeyID)
}
