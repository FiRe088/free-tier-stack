//go:build unix

package tailer

import (
	"os"
	"syscall"
)

// inodeOf returns the file's inode number, used to tell "the same file grew"
// apart from "the path now points at a different file" (log rotation).
func inodeOf(fi os.FileInfo) uint64 {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Ino)
	}
	return 0
}
