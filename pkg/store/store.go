package store

type Store interface {
	Get(key string) (string, error)
	Put(key, value string) error
	Delete(key string) error
	Keys() []string
}
