// Package tailer reads log files line by line and, in follow mode, keeps
// reading as they grow, surviving rotation and truncation.
//
// Every line carries the byte position just after it. The caller commits that
// position together with the rows it produced (one database transaction), so
// a restart resumes exactly where the last commit ended: no lost lines, no
// duplicates.
package tailer

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// Position identifies a point in a specific file. Inode distinguishes "the
// same file, grown" from "a new file at the same path" after rotation.
type Position struct {
	Inode  uint64
	Offset int64
}

// Line is one complete log line (without its trailing newline) and the
// position immediately after it.
type Line struct {
	Text string
	End  Position
}

// Options controls how Tail behaves at end of file.
type Options struct {
	// Follow keeps reading as the file grows instead of returning at EOF.
	Follow bool
	// PollInterval is how often to check for new data, rotation or
	// truncation once caught up. Defaults to 250ms.
	PollInterval time.Duration
	// MaxLineBytes caps how much of an unterminated line is buffered before
	// it is emitted anyway, so one runaway line cannot exhaust a 128MB
	// container. Defaults to 1 MiB.
	MaxLineBytes int
}

// Tail reads path starting at start and sends each complete line to out.
//
// The start position is honoured only if it refers to the file currently at
// path (same inode) and still fits inside it. Otherwise the file was rotated
// or truncated while we were stopped, and reading begins at offset 0.
//
// Without Follow, Tail returns nil at EOF; a final line with no trailing
// newline is still emitted. With Follow, an unterminated line is held until
// its newline arrives, because a writer may be mid-write. Tail then returns
// only when ctx is cancelled or on an unrecoverable error.
func Tail(ctx context.Context, path string, start Position, opts Options, out chan<- Line) error {
	if opts.PollInterval <= 0 {
		opts.PollInterval = 250 * time.Millisecond
	}
	if opts.MaxLineBytes <= 0 {
		opts.MaxLineBytes = 1 << 20
	}

	f, pos, err := openAt(path, start)
	if err != nil {
		return err
	}
	defer func() { f.Close() }()

	r := bufio.NewReaderSize(f, 64*1024)
	var pending []byte // bytes of a line whose newline hasn't arrived yet

	emit := func(text []byte, consumed int64) error {
		pos.Offset += consumed
		select {
		case out <- Line{Text: string(text), End: pos}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	for {
		chunk, rerr := r.ReadSlice('\n')
		pending = append(pending, chunk...)

		switch {
		case rerr == nil: // complete line
			n := int64(len(pending))
			if err := emit(bytes.TrimRight(pending, "\r\n"), n); err != nil {
				return nil
			}
			pending = pending[:0]
			continue

		case errors.Is(rerr, bufio.ErrBufferFull):
			if len(pending) >= opts.MaxLineBytes {
				n := int64(len(pending))
				if err := emit(pending, n); err != nil {
					return nil
				}
				pending = pending[:0]
			}
			continue

		case !errors.Is(rerr, io.EOF):
			return fmt.Errorf("tailer: reading %s: %w", path, rerr)
		}

		// EOF.
		if !opts.Follow {
			if len(pending) > 0 {
				if err := emit(bytes.TrimRight(pending, "\r\n"), int64(len(pending))); err != nil {
					return nil
				}
			}
			return nil
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(opts.PollInterval):
		}

		fi, statErr := os.Stat(path)
		if statErr != nil {
			// Path gone: rotated away and the new file isn't there yet.
			// Keep the old handle; the writer may still be appending to it.
			continue
		}

		if ino := inodeOf(fi); ino != 0 && ino != pos.Inode {
			// Rotated: path now names a different file. Drain whatever the
			// old file still holds, then switch.
			if err := drain(ctx, r, &pending, emit); err != nil {
				return nil
			}
			nf, npos, err := openAt(path, Position{})
			if err != nil {
				continue // new file vanished between stat and open; retry next poll
			}
			f.Close()
			f, pos, pending = nf, npos, pending[:0]
			r.Reset(f)
			continue
		}

		if fi.Size() < pos.Offset+int64(len(pending)) {
			// Truncated in place (e.g. logrotate copytruncate).
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				return fmt.Errorf("tailer: seeking %s after truncation: %w", path, err)
			}
			pos.Offset, pending = 0, pending[:0]
			r.Reset(f)
		}
	}
}

// drain reads the rest of a rotated-away file, emitting complete lines and
// any final unterminated line (nobody will append to it any more).
func drain(ctx context.Context, r *bufio.Reader, pending *[]byte, emit func([]byte, int64) error) error {
	for {
		chunk, err := r.ReadSlice('\n')
		*pending = append(*pending, chunk...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err == nil || len(*pending) > 0 && errors.Is(err, io.EOF) {
			n := int64(len(*pending))
			if eerr := emit(bytes.TrimRight(*pending, "\r\n"), n); eerr != nil {
				return eerr
			}
			*pending = (*pending)[:0]
		}
		if err != nil {
			return nil
		}
	}
}

// openAt opens path and seeks to start if start still describes this file.
func openAt(path string, start Position) (*os.File, Position, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, Position{}, fmt.Errorf("tailer: opening %s: %w", path, err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, Position{}, fmt.Errorf("tailer: stat %s: %w", path, err)
	}

	pos := Position{Inode: inodeOf(fi)}
	sameFile := start.Inode == pos.Inode
	if sameFile && start.Offset > 0 && start.Offset <= fi.Size() {
		if _, err := f.Seek(start.Offset, io.SeekStart); err != nil {
			f.Close()
			return nil, Position{}, fmt.Errorf("tailer: seeking %s: %w", path, err)
		}
		pos.Offset = start.Offset
	}
	return f, pos, nil
}
