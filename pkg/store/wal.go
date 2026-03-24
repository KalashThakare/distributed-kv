package store

import (
	"os"
	"sync"
)

type WAL struct {
	mu   sync.Mutex
	file *os.File
	path string
}