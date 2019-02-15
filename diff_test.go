package diff

import (
	"bytes"
	"testing"
)

func TestDiffAllChangeTypes(t *testing.T) {
	testChanges(t, []KeyValue{
		kvS("k1", "v1"),
		kvS("k2", "v2"),
		kvS("k3", "v3.1"),
	}, []KeyValue{
		kvS("k1", "v1"),
		kvS("k3", "v3"),
		kvS("k4", "v4"),
	}, []Change{
		{Unchanged, []byte("k1"), nil},
		{Created, []byte("k2"), []byte("v2")},
		{Modified, []byte("k3"), []byte("v3.1")},
		{Deleted, []byte("k4"), nil},
	})
}

func TestDiffEmpty(t *testing.T) {
	testChanges(t, []KeyValue{}, []KeyValue{}, []Change{})
}

func TestDiffCreate(t *testing.T) {
	testChanges(t, []KeyValue{
		kvS("k1", "v1"),
	}, []KeyValue{}, []Change{
		{Created, []byte("k1"), []byte("v1")},
	})
}

func TestDiffSame(t *testing.T) {
	testChanges(t, []KeyValue{
		kvS("k1", "v1"),
	}, []KeyValue{
		kvS("k1", "v1"),
	}, []Change{
		{Unchanged, []byte("k1"), nil},
	})
}

func TestDiffModify(t *testing.T) {
	testChanges(t, []KeyValue{
		kvS("k1", "v1.1"),
	}, []KeyValue{
		kvS("k1", "v1"),
	}, []Change{
		{Modified, []byte("k1"), []byte("v1.1")},
	})
}

func TestDiffDelete(t *testing.T) {
	testChanges(t, []KeyValue{}, []KeyValue{
		kvS("k1", "v1.1"),
	}, []Change{
		{Deleted, []byte("k1"), nil},
	})
}

func TestDiffIndexStream(t *testing.T) {
	cancel := make(chan bool, 1)
	changes := make(chan Change, 10)

	refIndex := NewIndex(true)
	refIndex.Index(kvS("k2", "v2"))

	go func() {
		DiffIndexStream(refIndex, stream([]KeyValue{
			kvS("k1", "v1"),
		}), changes, cancel)
		close(changes)
	}()

	expectChanges(t, changes, []Change{
		{Deleted, []byte("k1"), nil},
		{Created, []byte("k2"), []byte("v2")},
	})
}

func stream(kvs []KeyValue) <-chan KeyValue {
	c := make(chan KeyValue, 1)
	go func() {
		for _, kv := range kvs {
			c <- kv
		}
		close(c)
	}()
	return c
}

func kvS(key, value string) KeyValue {
	return KeyValue{[]byte(key), []byte(value)}
}

func testChanges(t *testing.T, ref, current []KeyValue, exp []Change) {
	t.Helper()

	cancel := make(chan bool, 1)
	changes := make(chan Change, 10)

	go func() {
		Diff(stream(ref), stream(current), changes, cancel)
		close(changes)
	}()

	expectChanges(t, changes, exp)
}

func expectChanges(t *testing.T, changes <-chan Change, exp []Change) {
	t.Helper()

	for change := range changes {
		found := false
		for idx, expChange := range exp {
			if eq(change, expChange) {
				exp = append(exp[0:idx], exp[idx+1:]...)
				found = true
			}
		}
		if !found {
			t.Error("unexpected change: ", change)
		}
	}

	for _, noFound := range exp {
		t.Error("expected change not found: ", noFound)
	}
}

func eq(c1, c2 Change) bool {
	if c1.Type != c2.Type {
		return false
	}
	if !bytes.Equal(c1.Key, c2.Key) {
		return false
	}
	if !bytes.Equal(c1.Value, c2.Value) {
		return false
	}
	return true
}
