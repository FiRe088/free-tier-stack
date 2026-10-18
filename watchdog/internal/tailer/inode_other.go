//go:build !unix

package tailer

import "os"

// inodeOf has no portable equivalent off Unix. Returning 0 disables
// rename-based rotation detection; truncation is still detected by size.
func inodeOf(fi os.FileInfo) uint64 { return 0 }
