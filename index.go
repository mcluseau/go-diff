package diff

type Index interface {
	Index(kv KeyValue)
	Compare(kv KeyValue) CompareResult
	KeysNotSeen() <-chan []byte
	Value(key []byte) []byte
	KeyValues() <-chan KeyValue
	DoesRecordValues() bool
}
