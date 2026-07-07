package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

var (
	ErrMissingAuth       = errors.New("missing authorization")
	ErrInvalidAuth       = errors.New("invalid authorization")
	ErrAccessKeyNotFound = errors.New("access key not found")
	ErrSignatureMismatch = errors.New("signature does not match")
)

type SigV4Verifier struct {
	keys KeyStore
}

func NewSigV4Verifier(keys KeyStore) *SigV4Verifier {
	return &SigV4Verifier{keys: keys}
}

func (v *SigV4Verifier) Verify(ctx context.Context, r *http.Request) (AccessKey, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return AccessKey{}, ErrMissingAuth
	}
	fields := parseAuthHeader(authHeader)
	credential := fields["Credential"]
	signedHeaders := fields["SignedHeaders"]
	signature := fields["Signature"]
	if credential == "" || signedHeaders == "" || signature == "" {
		return AccessKey{}, ErrInvalidAuth
	}
	parts := strings.Split(credential, "/")
	if len(parts) != 5 {
		return AccessKey{}, ErrInvalidAuth
	}
	accessKeyID, date, region, service := parts[0], parts[1], parts[2], parts[3]
	key, ok := v.keys.FindAccessKey(ctx, accessKeyID)
	if !ok || !key.Enabled {
		return AccessKey{}, ErrAccessKeyNotFound
	}
	amzDate := r.Header.Get("X-Amz-Date")
	if amzDate == "" {
		amzDate = r.Header.Get("Date")
	}
	if amzDate == "" {
		return AccessKey{}, ErrInvalidAuth
	}
	if t, err := time.Parse("20060102T150405Z", amzDate); err == nil {
		if time.Since(t) > 15*time.Minute || time.Until(t) > 15*time.Minute {
			return AccessKey{}, ErrInvalidAuth
		}
	}
	canonicalRequest, err := canonicalRequest(r, signedHeaders)
	if err != nil {
		return AccessKey{}, err
	}
	scope := strings.Join([]string{date, region, service, "aws4_request"}, "/")
	hash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(hash[:])
	expected := sign(key.SecretKey, date, region, service, stringToSign)
	if !hmac.Equal([]byte(expected), []byte(signature)) {
		return AccessKey{}, ErrSignatureMismatch
	}
	return key, nil
}

func canonicalRequest(r *http.Request, signedHeaders string) (string, error) {
	headerNames := strings.Split(signedHeaders, ";")
	var canonicalHeaders strings.Builder
	for _, name := range headerNames {
		value := ""
		if name == "host" {
			value = r.Host
		} else {
			value = strings.Join(r.Header.Values(http.CanonicalHeaderKey(name)), ",")
		}
		canonicalHeaders.WriteString(strings.ToLower(name))
		canonicalHeaders.WriteByte(':')
		canonicalHeaders.WriteString(strings.Join(strings.Fields(value), " "))
		canonicalHeaders.WriteByte('\n')
	}
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}
	return strings.Join([]string{
		r.Method,
		canonicalURI(r.URL.EscapedPath()),
		canonicalQuery(r.URL.Query()),
		canonicalHeaders.String(),
		signedHeaders,
		payloadHash,
	}, "\n"), nil
}

func canonicalURI(path string) string {
	if path == "" {
		return "/"
	}
	return path
}

func canonicalQuery(values url.Values) string {
	if len(values) == 0 {
		return ""
	}
	var parts []string
	for key, items := range values {
		sort.Strings(items)
		for _, item := range items {
			parts = append(parts, url.QueryEscape(key)+"="+url.QueryEscape(item))
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, "&")
}

func parseAuthHeader(header string) map[string]string {
	header = strings.TrimPrefix(header, "AWS4-HMAC-SHA256 ")
	out := map[string]string{}
	for _, part := range strings.Split(header, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) == 2 {
			out[kv[0]] = kv[1]
		}
	}
	return out
}

func sign(secret, date, region, service, stringToSign string) string {
	kDate := hmacSHA256([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, "aws4_request")
	return hex.EncodeToString(hmacSHA256(kSigning, stringToSign))
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(data))
	return mac.Sum(nil)
}

func CredentialScope(date, region, service string) string {
	return fmt.Sprintf("%s/%s/%s/aws4_request", date, region, service)
}
