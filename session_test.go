package session

import (
	"testing"
	"regexp"
)

func TestUUID(t *testing.T) {
	tt := make([]string, 100)
	re := regexp.MustCompile(`^[[:alnum:]]{8}(?:-[[:alnum:]]{4}){3}-[[:alnum:]]{12}$`)
	for i, _ := range tt {
		id, err := UUID()
		if err != nil {
			t.Fatalf("%d: %s", i, err.Error())
		}
		for _, old := range tt {
			if old == id {
				t.Fatalf("%d: dupliate ID %s", i, id)
			}
		}
		if ma := re.MatchString(id); !ma {
			t.Fatalf("%d: wrong UUID format %s", i, id)
		}
	}
}
