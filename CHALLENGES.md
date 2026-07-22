
## 2026-07-21 Missing file caused false "add dependency" error
**Problem:** `go build` suggested `go get` for an internal package
(`internal/tailer`), which should never happen for same-module imports.
**Root cause:** The `tailer.go` file was never actually created — a
heredoc command from an earlier step was skipped or didn't execute before
moving on to the next file.
**Fix:** Verified with `ls -la` that the file didn't exist, then re-ran
the heredoc to create it.
**Lesson:** Always verify a file was written (`cat` or `ls`) immediately
after creating it, before building on top of it — don't trust that a
command ran just because the terminal moved to the next prompt.
