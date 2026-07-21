# Build Log: Challenges & Errors

Chronological record of real problems hit during this build and how they
were resolved. Kept deliberately raw — this is engineering-process evidence,
not polished documentation.

Format per entry:

## [date] Short title
**Problem:**
**Root cause:**
**Fix:**
**Lesson:**

---

## 2026-07-21 WSL landed in Docker Desktop's internal distro, not Ubuntu
**Problem:** `wsl` command dropped into `/mnt/host/c/...`, not a real Ubuntu
home directory. `cat /etc/os-release` returned nothing.
**Root cause:** Docker Desktop was installed and registered as the only
WSL distro (`docker-desktop`), so plain `wsl` launched its internal utility
VM instead of a general-purpose Linux environment. No Ubuntu distro existed.
**Fix:** `wsl --install -d Ubuntu`, then `wsl --set-default Ubuntu`.
**Lesson:** `wsl --list --verbose` should be the first diagnostic command
any time WSL behavior looks wrong — don't assume the default distro is
what you think it is.

## 2026-07-21 go.mod version drift from a single go get
**Problem:** `go mod init` produced `go 1.25.0` in go.mod despite only Go
1.23.4 being installed locally.
**Root cause:** `go get github.com/jackc/pgx/v5/pgxpool` pulled in pgx
v5.10.0, which has a hard minimum requirement of Go 1.25.0. GOTOOLCHAIN=auto
silently downloaded 1.25.x to satisfy it.
**Fix:** Accepted 1.25.0 as the real floor (pgx requires it) and upgraded
the base Go install to match, instead of fighting the auto-toolchain
behavior by re-pinning to 1.23.4.
**Lesson:** A dependency's minimum Go version is a real constraint, not
noise — check `go.mod`'s `go` line output after every `go get`, not just
after `go mod init`.

## 2026-07-21 Go module path didn't match actual repo layout
**Problem:** `go mod init github.com/fire/watchdog` used a placeholder
username and assumed watchdog was its own top-level repo.
**Root cause:** Didn't confirm real GitHub username before initializing
the module, and didn't account for the monorepo layout (one repo,
multiple Go modules as subdirectories).
**Fix:** `go mod edit -module github.com/FiRe088/free-tier-stack/watchdog`
before enough code existed to make the fix expensive.
**Lesson:** Confirm module path against actual intended repo structure
before writing more than one file — retrofitting import paths across many
files is pure toil with zero engineering value.
