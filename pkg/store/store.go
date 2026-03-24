package store

import (
	"errors"
	"fmt"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// type Store interface {
// 	Get(key string) (string, error)
// 	Put(key, value string) error
// 	Delete(key string) error
// 	Keys() []string
// }

var ErrKeyNotFound = errors.New("key not found")
var bucketName = []byte("kv")

const FlushInterval = 30 * time.Second

type Store struct {
	mu        sync.RWMutex
	mem       map[string]string
	wal       *WAL
	db        *bolt.DB
	stopFlush chan struct{}
	wg        sync.WaitGroup
}

type Config struct {
	WALPath  string
	BoltPath string //file path where your database is stored on disk using bbolt (BoltDB).
}

// func Open(cfg Config) (*Store, error) {
// 	s := &Store{
// 		mem:       make(map[string]string),
// 		stopFlush: make(chan struct{}),
// 	}

// 	if cfg.BoltPath != "" {
// 		db, err := bolt.Open(cfg.BoltPath, 0600, &bolt.Options{
// 			Timeout: 1 * time.Second,
// 		})
// 		if err != nil {
// 			return nil, fmt.Errorf("open bbolt: %w", err)
// 		}

// 		s.db = db

// 		if err := s.loadFromBolt(); err != nil {
// 			return nil, fmt.Errorf("load from bbolt: %w", err)
// 		}

// 	}
// }

func (s *Store) loadFromBolt() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}

		return b.ForEach(func(k, v []byte) error {
			s.mem[string(k)] = string(v)
			return nil
		})
	})
}

func (s *Store) flushToBolt() error {
	s.mu.RLock()
	snapshot := make(map[string]string, len(s.mem))

	for k, v := range s.mem {
		snapshot[k] = v
	}

	s.mu.RUnlock()

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return fmt.Errorf("bucket %q not found", bucketName)
		}
		for k, v := range snapshot {
			if err := b.Put([]byte(k), []byte(v)); err != nil {
				return err
			}
		}
		return nil
	})
}
