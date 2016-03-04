package session

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"regexp"
	"bytes"
	"errors"
	"strings"
)

var ErrInvalidSignature = errors.New("session: cookie signature is invalid")

func Sign(unsigned string, key []byte) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(unsigned))
	h := mac.Sum(nil)
	b := make([]byte, base64.StdEncoding.EncodedLen(len(h)))
	base64.StdEncoding.Encode(b, h)
	return unsigned + "." + string(bytes.Join(regexp.MustCompile(`\w+`).FindAll(b, -1), nil))
}

func Unsign(signed string, key []byte) (string, error) {
	s := strings.Split(signed, ".")
	if len(s) != 2 || subtle.ConstantTimeCompare([]byte(Sign(s[0], key)), []byte(signed)) == 0 {
		return "", ErrInvalidSignature
	}
	return s[0], nil
}
