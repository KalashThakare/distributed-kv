package store

import (
	"errors"
	"fmt"
	"os"
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

func Open(cfg Config) (*Store, error) {
	s := &Store{
		mem:       make(map[string]string),
		stopFlush: make(chan struct{}),
	}

	if cfg.BoltPath != "" {
		db, err := bolt.Open(cfg.BoltPath, 0600, &bolt.Options{
			Timeout: 1 * time.Second,
		})
		if err != nil {
			return nil, fmt.Errorf("open bbolt: %w", err)
		}

		s.db = db

		if err := s.loadFromBolt(); err != nil {
			return nil, fmt.Errorf("load from bbolt: %w", err)
		}
	}

	if cfg.WALPath != "" {
		wal, err := openWAL(cfg.WALPath)
		if err != nil {
			return nil, fmt.Errorf("open WAL: %w", err)
		}

		s.wal = wal

		if err := s.wal.Replay(s.mem); err != nil {
			return nil, fmt.Errorf("replay WAL: %w", err)
		}
	}

	// Step 3: start background goroutine that flushes memory to bbolt
	if s.db != nil {
		s.wg.Add(1)
		go s.flushLoop()
	}
	return s, nil
}

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

func (s *Store) flushLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := s.flushToBolt(); err != nil {
				// Log the error but keep running — WAL protects us
				fmt.Fprintf(os.Stderr, "flush to bbolt: %v\n", err)
			} else if s.wal != nil {
				// Safe to truncate WAL after successful flush
				_ = s.wal.Truncate()
			}
		case <-s.stopFlush:
			return
		}
	}
}

func (s *Store) Put(key, value string) error {
	if s.wal != nil{
		if err := s.wal.Append(OpSet, key, value); err != nil{
			return fmt.Errorf("WAL: %w", err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mem[key] = value
	return nil
}

func (s *Store) Get(key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.mem[key]
	if !ok {
		return "", ErrKeyNotFound
	}
	return v, nil
} 

func (s *Store) Delete(key string) error {
	if s.wal != nil {
		if err := s.wal.Append(OpDelete, key, ""); err != nil{
			return fmt.Errorf("WAL append: %w", err)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.mem, key)
	return nil
}

func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.mem))
	for key := range s.mem{
		keys = append(keys, key)
	}

	return keys
}

func (s *Store) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.mem)
}


func (s *Store) Close() error {
	if s.db != nil{
		close(s.stopFlush)
		s.wg.Wait()

		if err := s.flushToBolt(); err != nil{
			return fmt.Errorf("final flush: %w", err)
		}
		if s.wal != nil{
			_ = s.wal.Truncate()
		}
		if err :=  s.db.Close(); err != nil{
			return fmt.Errorf("close bbolt: %w", err)
		}
	}

	if s.wal != nil{
		return s.wal.Close()
	}
	return nil
}