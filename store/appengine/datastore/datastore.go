package datastore

import (
	"time"
	"github.com/coldume/session"
	"github.com/coldume/session/store"
	"google.golang.org/appengine/datastore"
	"golang.org/x/net/context"
)

var _ store.Store = &Datastore{}

type Datastore struct {
	sessionKind, dataKind string
}

func NewDatastore(sessionKind, dataKind string) *Datastore {
	return &Datastore{sessionKind: sessionKind, dataKind: dataKind}
}

func (ds *Datastore) Get(ctx context.Context, id string) (*session.Session, error) {
	k := datastore.NewKey(ctx, ds.sessionKind, id, 0, nil)
	s := &Session{}
	if err := datastore.Get(ctx, k, s); err != nil {
		if err != datastore.ErrNoSuchEntity {
			return nil, err
		}
		return nil, store.ErrNoSuchSession
	}
	kk := datastore.NewKey(ctx, ds.dataKind, id, 0, k)
	d := make(Data)
	if err := datastore.Get(ctx, kk, d); err != nil {
		return nil, err
	}
	sess := session.NewSession(id, s.Duration, time.Unix(0, s.Expires))
	for k, v := range d {
		sess.Set(k, v)
	}
	return sess, nil
}

func (ds *Datastore) Put(ctx context.Context, sess *session.Session) error {
	return datastore.RunInTransaction(ctx, func(ctx context.Context) error {
		k := datastore.NewKey(ctx, ds.sessionKind, sess.ID, 0, nil)
		s := &Session{Duration: sess.Duration, Expires: sess.Expires.UnixNano()}
		if _, err := datastore.Put(ctx, k, s); err != nil {
			return err
		}
		kk := datastore.NewKey(ctx, ds.dataKind, sess.ID, 0, k)
		d := make(Data)
		for n, v := range sess.All() {
			d[n] = v
		}
		if _, err := datastore.Put(ctx, kk, d); err != nil {
			return err
		}
		return nil
	}, nil)
}

func (ds *Datastore) Del(ctx context.Context, id string) error {
	k := datastore.NewKey(ctx, ds.sessionKind, id, 0, nil)
	kk := datastore.NewKey(ctx, ds.dataKind, id, 0, k)
	return datastore.RunInTransaction(ctx, func(ctx context.Context) error {
		return datastore.DeleteMulti(ctx, []*datastore.Key{k, kk})
	}, nil)
}

func (ds *Datastore) Clear(ctx context.Context) error {
	ite := datastore.NewQuery(ds.sessionKind).KeysOnly().Run(ctx)
	for {
		k, err := ite.Next(nil)
		if err == datastore.Done {
			break
		}
		if err != nil {
			return err
		}
		if err := ds.Del(ctx, k.StringID()); err != nil {
			return err
		}
	}
	return nil
}

func (ds *Datastore) Clean(ctx context.Context) error {
	e := time.Now().UnixNano()
	ite := datastore.NewQuery(ds.sessionKind).Filter("Expires <=", e).KeysOnly().Run(ctx)
	for {
		k, err := ite.Next(nil)
		if err == datastore.Done {
			break
		}
		if err != nil {
			return err
		}
		if err := ds.Del(ctx, k.StringID()); err != nil {
			return err
		}
	}
	return nil
}

type Session struct {
	Duration time.Duration
	Expires  int64
}

var _ datastore.PropertyLoadSaver = make(Data)

type Data map[string]interface{}

func (data Data) Load(ps []datastore.Property) error {
	for _, p := range ps {
		data[p.Name] = p.Value
	}
	return nil
}

func (data Data) Save() ([]datastore.Property, error) {
	ps := make([]datastore.Property, 0)
	for n, v := range data {
		ps = append(ps, datastore.Property{Name: n, Value: v})
	}
	return ps, nil
}
