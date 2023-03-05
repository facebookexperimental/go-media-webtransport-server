package memfile

import (
	"io"
	"regexp"
	"strconv"
	"sync"
	"time"
)

// Decoding headers
type FileHeader struct {
	CacheControl  string `json:"Cache-Control"`
	MediaType     string `json:"Joc-Media-Type"`
	Timestamp     int64  `json:"Joc-Timestamp"`
	Duration      int64  `json:"Joc-Duration"`
	ChunkType     string `json:"Joc-Chunk-Type"`
	SeqId         int64  `json:"Joc-Seq-Id"`
	FirstFrameClk int64  `json:"Joc-First-Frame-Clk"`
	UniqueId      string `json:"Joc-Uniq-Id"`
}

// File Definition of file
type MemFile struct {
	Name       string
	Headers    FileHeader
	ReceivedAt time.Time
	MaxAgeS    int64
	Eof        bool

	buffer []byte
	lock   *sync.RWMutex
}

// FileReader Defines a reader
type fileReadCloser struct {
	offset int
	*MemFile
}

// NewFile Creates a new file
func New(headers FileHeader) *MemFile {
	f := MemFile{
		Headers:    headers,
		lock:       new(sync.RWMutex),
		buffer:     []byte{},
		Eof:        false,
		ReceivedAt: time.Now(),
		MaxAgeS:    getMaxAgeOr(headers.CacheControl, -1),
	}
	return &f
}

// Read Reads bytes from filereader
func (r *fileReadCloser) Read(p []byte) (int, error) {
	r.MemFile.lock.RLock()
	defer r.MemFile.lock.RUnlock()
	if r.offset >= len(r.MemFile.buffer) {
		if r.MemFile.Eof {
			return 0, io.EOF
		}
		return 0, nil
	}
	n := copy(p, r.MemFile.buffer[r.offset:])
	r.offset += n
	return n, nil
}

// NewReadCloser Crates a new filereader from a file
func (f *MemFile) NewReadCloser() io.ReadCloser {
	f.lock.RLock()
	defer f.lock.RUnlock()

	return &fileReadCloser{
		offset:  0,
		MemFile: f,
	}
}

// Close Closes a file
func (f *MemFile) Close() error {
	return f.closeInternal(true)
}
func (f *MemFile) CloseNotFinished() error {
	return f.closeInternal(false)
}
func (f *MemFile) closeInternal(finished bool) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.Eof = finished

	return nil
}

// Write Write bytes to a file
func (f *MemFile) Write(p []byte) (int, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.buffer = append(f.buffer, p...)
	return len(p), nil
}

// Helper
func getMaxAgeOr(s string, def int64) int64 {
	ret := def
	r := regexp.MustCompile(`max-age=(?P<maxage>\d*)`)
	match := r.FindStringSubmatch(s)
	for i, name := range r.SubexpNames() {
		if i > 0 && i <= len(match) {
			if name == "maxage" {
				valInt, err := strconv.ParseInt(match[i], 10, 64)
				if err == nil {
					ret = valInt
					break
				}
			}
		}
	}
	return ret
}
