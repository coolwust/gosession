package session

import "golang.org/x/net/context"

const contextKey = iota

func NewContext(sess *Session) context.Context {
	return context.WithValue(context.Background(), contextKey, sess)
}

func WithContext(parent context.Context, sess *Session) context.Context {
	return context.WithValue(parent, contextKey, sess)
}

func FromContext(ctx context.Context) *Session {
	return ctx.Value(contextKey).(*Session)
}
