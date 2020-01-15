package diff

import (
	"errors"
	"fmt"
	"log"
	"sync"
)

var (
	Debug = false

	ErrMustSupportKeysNotSeen = errors.New("referenceIndex must support KeysNotSeen")
	ErrMustRecordValues       = errors.New("referenceIndex must record values")
)

// Compares a store (currentValues) with a reference (referenceValues), streaming the reference.
//
// The referenceValues channel provide values in the reference store. It will be indexed.
//
// The currentValues channel provide values in the target store. It will be indexed.
//
// The changes channel will receive the changes, including Unchanged.
//
// See other diff implementations for faster and less memory consumming alternatives if
// you can provide better garanties from your stores.
func Diff(referenceValues, currentValues <-chan KeyValue, changes chan Change, cancel <-chan bool) error {
	referenceIndex := NewIndex(true)
	currentIndex := NewIndex(false)

	wg := sync.WaitGroup{}
	wg.Add(2)

	var err1, err2 error

	go func() {
		defer wg.Done()
		err1 = referenceIndex.Index(referenceValues, nil)
	}()

	go func() {
		defer wg.Done()
		err2 = currentIndex.Index(currentValues, nil)
	}()

	wg.Wait()

	if err1 != nil {
		return fmt.Errorf("error indexing reference values: %v", err1)
	}

	if err2 != nil {
		return fmt.Errorf("error indexing current values: %v", err2)
	}

	return DiffIndexIndex(referenceIndex, currentIndex, changes, cancel)
}

// Compares a store (currentValues) with a reference (referenceValues), streaming the reference.
//
// The referenceValues channel provide values in the reference store. It MUST NOT produce duplicate keys.
//
// The currentValues channel provide values in the target store. It will be indexed.
//
// The changes channel will receive the changes, including Unchanged.
func DiffStreamReference(referenceValues, currentValues <-chan KeyValue, changes chan Change, cancel <-chan bool) error {
	currentIndex := NewIndex(false)

	if err := currentIndex.Index(currentValues, nil); err != nil {
		return err
	}

	return DiffStreamIndex(referenceValues, currentIndex, changes, cancel)
}

// Compares a store (currentIndex) with a reference (referenceValues), streaming the reference.
//
// The referenceValues channel provide values in the reference store. It MUST NOT produce duplicate keys.
//
// The currentIndex is the indexed target store.
//
// The changes channel will receive the changes, including Unchanged.
func DiffStreamIndex(referenceValues <-chan KeyValue, currentIndex Index, changes chan Change, cancel <-chan bool) error {
	if Debug {
		log.Print("DiffStreamIndex: starting")
		defer log.Print("DiffStreamIndex: finished")
	}

l:
	for {
		var (
			kv KeyValue
			ok bool
		)

		select {
		case <-cancel:
			if Debug {
				log.Print("DiffStreamIndex: cancelled")
			}
			return nil

		case kv, ok = <-referenceValues:
			if !ok {
				if Debug {
					log.Print("DiffStreamIndex: end of values")
				}
				break l
			}
		}

		if Debug {
			log.Print("DiffStreamIndex: new value")
		}

		cmp, err := currentIndex.Compare(kv)
		if err != nil {
			return err
		}

		switch cmp {
		case MissingKey:
			changes <- Change{
				Type:  Created,
				Key:   kv.Key,
				Value: kv.Value,
			}

		case ModifiedKey:
			changes <- Change{
				Type:  Modified,
				Key:   kv.Key,
				Value: kv.Value,
			}

		case UnchangedKey:
			changes <- Change{
				Type: Unchanged,
				Key:  kv.Key,
			}

		}
	}

	keysNotSeen := currentIndex.KeysNotSeen()
	if keysNotSeen == nil {
		// not supported by the index
		return nil
	}

	if Debug {
		log.Print("DiffStreamIndex: deletion phase")
	}

	for key := range keysNotSeen {
		changes <- Change{
			Type: Deleted,
			Key:  key,
		}
	}

	return nil
}

// Compares a store (currentValues) with a reference (referenceIndex), streaming the reference.
//
// The referenceIndex is the indexed target store. It MUST store the values AND support KeysNotSeen.
//
// The currentValues channel provide values in the reference store. It MUST NOT produce duplicate keys.
//
// The changes channel will receive the changes, including Unchanged.
func DiffIndexStream(referenceIndex Index, currentValues <-chan KeyValue, changes chan Change, cancel <-chan bool) error {
	if !referenceIndex.DoesRecordValues() {
		return ErrMustRecordValues
	}
l:
	for {
		var (
			kv KeyValue
			ok bool
		)

		select {
		case <-cancel:
			return nil

		case kv, ok = <-currentValues:
			if !ok {
				break l
			}
		}

		cmp, err := referenceIndex.Compare(kv)
		if err != nil {
			return err
		}

		switch cmp {
		case MissingKey:
			changes <- Change{
				Type: Deleted,
				Key:  kv.Key,
			}

		case ModifiedKey:
			changes <- Change{
				Type:  Modified,
				Key:   kv.Key,
				Value: kv.Value,
			}

		case UnchangedKey:
			changes <- Change{
				Type: Unchanged,
				Key:  kv.Key,
			}

		}
	}

	keysNotSeen := referenceIndex.KeysNotSeen()
	if keysNotSeen == nil {
		// not supported by the index
		return ErrMustSupportKeysNotSeen
	}

	for key := range keysNotSeen {
		changes <- Change{
			Type:  Created,
			Key:   key,
			Value: referenceIndex.Value(key),
		}
	}

	return nil
}

// Compares a store (currentValues) with a reference (referenceIndex), streaming the reference.
//
// The referenceIndex is the indexed target store. It MUST store the values.
//
// The currentIndex is the indexed target store.
//
// The changes channel will receive the changes, including Unchanged.
func DiffIndexIndex(referenceIndex Index, currentIndex Index, changes chan Change, cancel <-chan bool) error {
	return DiffStreamIndex(referenceIndex.KeyValues(), currentIndex, changes, cancel)
}
