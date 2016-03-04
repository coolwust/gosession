package datastore

import (
	"testing"
	"reflect"
	"time"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/aetest"
	"github.com/coldume/session/store"
	"github.com/coldume/session"
	"golang.org/x/net/context"
)

var dataTest = Data{
	"foo": int64(1),
	"bar": float64(1.5),
	"baz": "qux",
}

func TestData(t *testing.T) {
	ctx, done, err := aetest.NewContext()
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	k := datastore.NewIncompleteKey(ctx, "data", nil)
	if k, err = datastore.Put(ctx, k, dataTest); err != nil {
		t.Fatal(err)
	}
	d := make(Data)
	if err := datastore.Get(ctx, k, d); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(dataTest, d) {
		t.Fatalf("got %v, want %v", d, dataTest)
	}
}

func TestDatastore(t *testing.T) {
	ctx, done, err := aetest.NewContext()
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	ds := NewDatastore("session", "sessionData")

	// Del non-existent ID
	if err := ds.Del(ctx, "100"); err != nil {
		t.Fatal("got %v, want nil", err)
	}

	// Put, Get, Del
	sess1 := session.NewSession("100", time.Duration(0), time.Now())
	sess1.Set("foo", "bar")
	if err := ds.Put(ctx, sess1); err != nil {
		t.Fatal(err)
	}
	sess2, err := ds.Get(ctx, "100")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sess1, sess2) {
		t.Fatalf("got %v, want %v", sess1, sess2)
	}
	if err := ds.Del(ctx, "100"); err != nil {
		t.Fatal(err)
	}
	if _, err := ds.Get(ctx, "100"); err != store.ErrNoSuchSession {
		t.Fatalf("got %v, want %v", err, store.ErrNoSuchSession)
	}
	if n, _ := datastore.NewQuery(ds.dataKind).Count(ctx); n != 0 {
		t.Fatalf("got %v, want 0", n)
	}

	// Clear
	ds.Put(ctx, session.NewSession("100", time.Duration(0), time.Now()))
	ds.Put(ctx, session.NewSession("200", time.Duration(0), time.Now()))
	ds.apply(ctx, "100", "200")
	if err := ds.Clear(ctx); err != nil {
		t.Fatal(err)
	}
	ds.apply(ctx, "100", "200")
	if n, _ := datastore.NewQuery(ds.sessionKind).Count(ctx); n != 0 {
		t.Fatalf("got %v, want 0", n)
	}
	if n, _ := datastore.NewQuery(ds.dataKind).Count(ctx); n != 0 {
		t.Fatalf("got %v, want 0", n)
	}

	// Clean
	ds.Put(ctx, session.NewSession("100", time.Duration(0), time.Now().Add(-time.Hour)))
	ds.Put(ctx, session.NewSession("200", time.Duration(0), time.Now().Add(time.Hour)))
	ds.apply(ctx, "100", "200")
	if err := ds.Clean(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ds.Get(ctx, "100"); err != store.ErrNoSuchSession {
		t.Fatalf("got %v, want %v", err, store.ErrNoSuchSession)
	}
	if _, err := ds.Get(ctx, "200"); err != nil {
		t.Fatal(err)
	}
}

// https://cloud.google.com/appengine/docs/go/datastore/#Go_Datastore_writes_and_data_visibility
func (ds *Datastore) apply(ctx context.Context, ids ...string) {
	for _, id := range ids {
		k := datastore.NewKey(ctx, ds.sessionKind, id, 0, nil)
		s := Session{}
		datastore.Get(ctx, k, &s)
		kk := datastore.NewKey(ctx, ds.dataKind, id, 0, k)
		d := make(Data)
		datastore.Get(ctx, kk, &d)
	}
}
