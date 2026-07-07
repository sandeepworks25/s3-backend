package s3api

import (
	"encoding/xml"
	"net/http"
)

type s3Error struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId"`
}

func writeS3Error(w http.ResponseWriter, status int, code, message, resource string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_ = xml.NewEncoder(w).Encode(s3Error{
		Code:      code,
		Message:   message,
		Resource:  resource,
		RequestID: requestID(),
	})
}

func requestID() string {
	return "req-" + randomHex(12)
}
