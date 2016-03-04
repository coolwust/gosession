package session

import (
	"time"
	"sync"
	"crypto/rand"
	"fmt"
)

type Session struct {
	ID       string
	Duration time.Duration
	Expires  time.Time
	data     map[string]interface{}
	mu       sync.RWMutex
}

func NewSession(id string, du time.Duration, ex time.Time) *Session {
	return &Session{
		ID:       id,
		Duration: du,
		Expires:  ex,
		data:     make(map[string]interface{}),
	}
}

// For best storage compatibility, only int64, float64, and string types are allowed
func (sess *Session) Set(k string, v interface{}) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	sess.data[k] = v
}

func (sess *Session) Get(k string) interface{} {
	sess.mu.RLock()
	defer sess.mu.RUnlock()
	return sess.data[k]
}

func (sess *Session) Del(k string) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	delete(sess.data, k)
}

func (sess *Session) All() map[string]interface{} {
	sess.mu.RLock()
	defer sess.mu.RUnlock()
	m := make(map[string]interface{})
	for k, v := range sess.data {
		m[k] = v
	}
	return m
}

func (sess *Session) Touch() {
	sess.Expires = time.Now().Add(sess.Duration)
}

func UUID() (string, error) {
	s := make([]byte, 16)
	if _, err := rand.Read(s); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", s[:4], s[4:6], s[6:8], s[8:10], s[10:16]), nil
}
