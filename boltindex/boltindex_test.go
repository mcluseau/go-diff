package boltindex

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/boltdb/bolt"
)

func TestKeysNotSeen(t *testing.T) {
	withDB(t, func(db *bolt.DB) {
		idx, err := New(db, []byte("test"), true)
		if err != nil {
			t.Fatal(err)
		}

		idx.Index(KeyValue{Key: []byte("a"), Value: []byte("value a")}, nil)

		k := <-idx.KeysNotSeen()
		if ks := string(k); ks != "a" {
			t.Errorf("did not read \"a\" but %q", ks)
		}

		idx.Index(KeyValue{Key: []byte("b"), Value: []byte("value b")}, nil)

		idx.Compare(KeyValue{Key: []byte("a"), Value: []byte("not value a")})

		kns := idx.KeysNotSeen()
		k = <-kns
		if ks := string(k); ks != "b" {
			t.Errorf("did not read \"b\" but %q", ks)
		}

		k, ok := <-kns
		if ok {
			t.Errorf("did not expect more values but got %q", string(k))
		}
	})
}

func withDB(t *testing.T, do func(db *bolt.DB)) {
	f, err := ioutil.TempFile(os.TempDir(), "boltindex-test-")
	if err != nil {
		t.Fatal(err)
	}

	defer os.Remove(f.Name())

	db, err := bolt.Open(f.Name(), 0644, nil)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()
	do(db)
}
