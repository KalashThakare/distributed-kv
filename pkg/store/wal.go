package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

type WAL struct {
	mu   sync.Mutex
	file *os.File
	path string
}

type Op byte

const (
	OpSet    Op = 0x01 //PUT operation
	OpDelete Op = 0x02 //DELETE operation
)

func openWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: f,
		path: path,
	}, nil
}

func (w *WAL) Replay(dst map[string]string) error {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	r := bufio.NewReader(w.file)
	for {
		header := make([]byte, 9)
		if _, err := io.ReadFull(r, header); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("WAL read header: %w", err)
		}
		op := Op(header[0])

		klen := binary.BigEndian.Uint32(header[1:5])
		vlen := binary.BigEndian.Uint32(header[5:9])

		// Read key
		key := make([]byte, klen)
		if _, err := io.ReadFull(r, key); err != nil {
			return fmt.Errorf("WAL read key: %w", err)
		}
		// Read value (empty for DELETE)
		var value []byte
		if vlen > 0 {
			value = make([]byte, vlen)
			if _, err := io.ReadFull(r, value); err != nil {
				return fmt.Errorf("WAL read value: %w", err)
			}
		}
		// Apply the operation to the destination map
		switch op {
		case OpSet:
			dst[string(key)] = string(value)
		case OpDelete:
			delete(dst, string(key))
		}
	}
	return nil
}

func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.file.Truncate(0); err != nil {
		return err
	}
	_, err := w.file.Seek(0, io.SeekStart)
	return err
}
