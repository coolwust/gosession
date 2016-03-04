package session

import (
	"testing"
)

var signTests = []struct {
	unsigned string
	key      []byte
	signed   string
}{
	{"hello", []byte("foo"), "hello.NAJWxCdrLTSD8CvzyLdIDhLH6pcCsiAKldMySgs4"},
	{"world", []byte("bar"), "world.6kpaLfNqzist38XcIeU9ejULKGCJK7u23K9Qwbyb4sk"},
}

func TestSign(t *testing.T) {
	for i, tt := range signTests {
		if signed := Sign(tt.unsigned, tt.key); signed != tt.signed {
			t.Errorf("%d: Sign(%#v, %#v) = %#v, want %#v", i, tt.unsigned, tt.key, signed, tt.signed)
		}
	}
}

var unsignTests = []struct {
	signed   string
	key      []byte
	unsigned string
	err      error
}{
	{"hello.NAJWxCdrLTSD8CvzyLdIDhLH6pcCsiAKldMySgs4",    []byte("foo"), "hello", nil},
	{"world.6kpaLfNqzist38XcIeU9ejULKGCJK7u23K9Qwbyb4sk", []byte("bar"), "world", nil},
	{"hello.6kpaLfNqzist38XcIeU9ejULKGCJK7u23K9Qwbyb4sk", []byte("foo"), "",      ErrInvalidSignature},
	{"world.6kpaLfNqzist38XcIeU9ejULKGCJK7u23K9Qwbyb4sk", []byte("foo"), "",      ErrInvalidSignature},
}

func TestUnsign(t *testing.T) {
	for i, tt := range unsignTests {
		if unsigned, err := Unsign(tt.signed, tt.key); unsigned != tt.unsigned || err != tt.err {
			t.Errorf("%d: Unsign(%#v, %#v) = %#v, %#v, want %#v, %#v", i, tt.signed, tt.key, unsigned, err, tt.unsigned, tt.err)
		}
	}
}
