package tailer

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const fastPoll = 10 * time.Millisecond

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func appendFile(t *testing.T, path, content string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(content); err != nil {
		t.Fatal(err)
	}
}

func inode(t *testing.T, path string) uint64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return inodeOf(fi)
}

// readAll runs Tail without follow and returns every line it emitted.
func readAll(t *testing.T, path string, start Position) []Line {
	t.Helper()
	out := make(chan Line, 1000)
	if err := Tail(context.Background(), path, start, Options{}, out); err != nil {
		t.Fatal(err)
	}
	close(out)
	var lines []Line
	for l := range out {
		lines = append(lines, l)
	}
	return lines
}

// next waits for one line from a following Tail.
func next(t *testing.T, out <-chan Line) Line {
	t.Helper()
	select {
	case l := <-out:
		return l
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a line")
		return Line{}
	}
}

// expectNothing asserts no line arrives for a short while.
func expectNothing(t *testing.T, out <-chan Line) {
	t.Helper()
	select {
	case l := <-out:
		t.Fatalf("unexpected line %q", l.Text)
	case <-time.After(100 * time.Millisecond):
	}
}

func follow(t *testing.T, path string, start Position) (<-chan Line, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan Line, 100)
	done := make(chan error, 1)
	go func() { done <- Tail(ctx, path, start, Options{Follow: true, PollInterval: fastPoll}, out) }()
	stop := func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Tail returned %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("Tail did not stop after cancel")
		}
	}
	return out, stop
}

func TestBatchModeReadsEverythingWithPositions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.log")
	writeFile(t, path, "one\ntwo\r\nthree") // CRLF and no trailing newline

	lines := readAll(t, path, Position{})
	want := []struct {
		text string
		end  int64
	}{{"one", 4}, {"two", 9}, {"three", 14}}

	if len(lines) != len(want) {
		t.Fatalf("got %d lines, want %d", len(lines), len(want))
	}
	ino := inode(t, path)
	for i, w := range want {
		if lines[i].Text != w.text || lines[i].End.Offset != w.end || lines[i].End.Inode != ino {
			t.Errorf("line %d = %+v, want text=%q end=%d inode=%d", i, lines[i], w.text, w.end, ino)
		}
	}
}

func TestResumesFromSavedOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.log")
	writeFile(t, path, "one\ntwo\nthree\n")

	lines := readAll(t, path, Position{Inode: inode(t, path), Offset: 4})
	if len(lines) != 2 || lines[0].Text != "two" || lines[1].Text != "three" {
		t.Fatalf("got %+v, want two and three", lines)
	}
}

func TestRestartAtEndReadsNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.log")
	writeFile(t, path, "one\ntwo\n")

	if lines := readAll(t, path, Position{Inode: inode(t, path), Offset: 8}); len(lines) != 0 {
		t.Fatalf("re-run re-read %d lines; ingestion is not idempotent", len(lines))
	}
}

func TestSavedOffsetIgnoredForDifferentFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.log")
	writeFile(t, path, "one\ntwo\n")

	// Saved position belongs to some other file (rotated while we were down).
	lines := readAll(t, path, Position{Inode: inode(t, path) + 1, Offset: 4})
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2 (start from 0 for a new file)", len(lines))
	}
}

func TestSavedOffsetBeyondEndStartsOver(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.log")
	writeFile(t, path, "one\n")

	// Same inode but the file is now shorter: it was truncated while we were down.
	lines := readAll(t, path, Position{Inode: inode(t, path), Offset: 500})
	if len(lines) != 1 || lines[0].Text != "one" {
		t.Fatalf("got %+v, want [one]", lines)
	}
}

func TestFollowPicksUpAppendsAndHoldsPartialLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.log")
	writeFile(t, path, "one\n")
	out, stop := follow(t, path, Position{})
	defer stop()

	if l := next(t, out); l.Text != "one" {
		t.Fatalf("got %q", l.Text)
	}

	appendFile(t, path, "half a li") // writer is mid-line
	expectNothing(t, out)

	appendFile(t, path, "ne\ntwo\n")
	if l := next(t, out); l.Text != "half a line" || l.End.Offset != 16 {
		t.Fatalf("got %+v, want 'half a line' ending at 16", l)
	}
	if l := next(t, out); l.Text != "two" || l.End.Offset != 20 {
		t.Fatalf("got %+v, want 'two' ending at 20", l)
	}
}

func TestFollowHandlesRenameRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeFile(t, path, "old-1\n")
	out, stop := follow(t, path, Position{})
	defer stop()

	if l := next(t, out); l.Text != "old-1" {
		t.Fatalf("got %q", l.Text)
	}
	oldIno := inode(t, path)

	// Writer appends one more line, then logrotate renames the file and the
	// app starts a fresh one at the same path.
	appendFile(t, path, "old-2\n")
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeFile(t, path, "new-1\n")
	newIno := inode(t, path)
	if newIno == oldIno {
		t.Skip("filesystem reused the inode; rotation by inode can't be observed here")
	}

	got := []Line{next(t, out), next(t, out)}
	if got[0].Text != "old-2" || got[0].End.Inode != oldIno {
		t.Fatalf("expected remainder of rotated file first, got %+v", got[0])
	}
	if got[1].Text != "new-1" || got[1].End.Inode != newIno || got[1].End.Offset != 6 {
		t.Fatalf("expected first line of new file at offset 6 with new inode, got %+v", got[1])
	}
}

func TestFollowHandlesTruncation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	writeFile(t, path, "aaaaaaaaaa\nbbbbbbbbbb\n")
	out, stop := follow(t, path, Position{})
	defer stop()
	next(t, out)
	next(t, out)

	// copytruncate: same file, emptied, then written again.
	if err := os.Truncate(path, 0); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * fastPoll) // let the tailer notice the shrink
	appendFile(t, path, "c\n")

	if l := next(t, out); l.Text != "c" || l.End.Offset != 2 {
		t.Fatalf("got %+v, want 'c' ending at offset 2", l)
	}
}

func TestFollowStopsOnCancel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.log")
	writeFile(t, path, "")
	_, stop := follow(t, path, Position{})
	stop() // fails the test if Tail doesn't return promptly
}

func TestMissingFileIsAnError(t *testing.T) {
	out := make(chan Line, 1)
	if err := Tail(context.Background(), filepath.Join(t.TempDir(), "nope.log"), Position{}, Options{}, out); err == nil {
		t.Fatal("expected error for missing file")
	}
}
