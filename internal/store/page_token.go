package store

import (
	"encoding/base64"
	"strconv"
)

// parseOffsetToken decodes an opaque page token into a numeric offset.
func parseOffsetToken(s string) (int, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(string(b))
}

// formatOffsetToken encodes a numeric offset into an opaque page token.
func formatOffsetToken(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}
