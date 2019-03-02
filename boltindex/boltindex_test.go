package boltindex

import (
	"bytes"
	"io/ioutil"
	"os"
	"runtime"
	"testing"

	"github.com/boltdb/bolt"
)

func TestKeysNotSeen(t *testing.T) {
	withDB(t, func(db *bolt.DB) {
		// -------------------------------------------------------------
		idx, err := New(db, []byte("test"), true)
		if err != nil {
			t.Fatal(err)
		}

		idx.Index(singleValue("a", "value a"), nil)

		k := <-idx.KeysNotSeen()
		if ks := string(k); ks != "a" {
			t.Errorf("did not read \"a\" but %q", ks)
		}

		idx.Cleanup()

		// -------------------------------------------------------------
		idx, err = New(db, []byte("test"), true)
		if err != nil {
			t.Fatal(err)
		}
		idx.Index(singleValue("b", "value b"), nil)

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

		idx.Cleanup()
	})
}

func TestInterrupted(t *testing.T) {
	withDB(t, func(db *bolt.DB) {
		idx, err := New(db, []byte("test"), true)
		if err != nil {
			t.Fatal(err)
		}
		idx.Index(singleValue("b", "value b"), nil)
		idx.Cleanup()

		idx, err = New(db, []byte("test"), true)
		if err != nil {
			t.Fatal(err)
		}

		defer idx.Cleanup()
		idx.Compare(KeyValue{Key: []byte("a"), Value: []byte("not value a")})
	})
}

func singleValue(key, value string) (ch chan KeyValue) {
	ch = make(chan KeyValue, 1)
	ch <- KeyValue{Key: []byte(key), Value: []byte(value)}
	close(ch)
	return
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

	buf := make([]byte, 64<<10)
	buf = buf[:runtime.Stack(buf, true)]

	if bytes.Contains(buf, []byte("writeSeen")) {
		t.Error("writeSeen left running:\n", string(buf))
	}
}
