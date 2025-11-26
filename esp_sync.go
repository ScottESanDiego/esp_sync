package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
)

// --- Configuration Constants ---

const (
	debounceInterval = 500 * time.Millisecond // Time to wait for more events before syncing
	maxRetries       = 5                      // Number of times to retry a failed file operation
)

// --- Custom Flag Type for String Array ---

type stringArray []string

func (i *stringArray) String() string {
	return fmt.Sprintf("%v", *i)
}

func (i *stringArray) Set(value string) error {
	if value != "" {
		for _, part := range strings.Split(value, ",") {
			*i = append(*i, strings.TrimSpace(part))
		}
	}
	return nil
}

// --- Global Variables ---

var (
	SOURCE_DIR  string
	DEST_DIR    string
	dryRun      bool
	ignoredDirs stringArray
	watcher     *fsnotify.Watcher
	wg          sync.WaitGroup // Waits for active sync operations on shutdown
)

// --- Logging Helper ---

func dryRunLog(format string, v ...interface{}) {
	if dryRun {
		log.Printf("[DRY-RUN] "+format, v...)
	} else {
		log.Printf(format, v...)
	}
}

// --- Path Helpers ---

func getDestPath(srcPath string) (string, error) {
	relPath, err := filepath.Rel(SOURCE_DIR, srcPath)
	if err != nil {
		return "", fmt.Errorf("could not get relative path: %w", err)
	}
	return filepath.Join(DEST_DIR, relPath), nil
}

func isPathIgnored(path string) bool {
	cleanPath := filepath.Clean(path)
	for _, ignoredPath := range ignoredDirs {
		if cleanPath == ignoredPath {
			return true
		}
		if strings.HasPrefix(cleanPath, ignoredPath+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

// --- Checksum & Verification Logic ---

// calculateMD5 computes the MD5 hash of a file.
func calculateMD5(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

// areFilesIdenticalRobust checks if files are identical using Size and MD5 Checksum.
// This is safer than ModTime for FAT32.
func areFilesIdenticalRobust(srcPath, destPath string) bool {
	srcInfo, err := os.Stat(srcPath)
	if err != nil {
		return false
	}
	destInfo, err := os.Stat(destPath)
	if os.IsNotExist(err) {
		return false
	}
	if err != nil {
		return false
	}

	// 1. Fast Check: Size
	if srcInfo.Size() != destInfo.Size() {
		return false
	}

	// 2. Robust Check: Content Hash
	// Since size matches, we must verify content to be sure.
	srcHash, err := calculateMD5(srcPath)
	if err != nil {
		return false // Assume different on error
	}
	destHash, err := calculateMD5(destPath)
	if err != nil {
		return false
	}

	return srcHash == destHash
}

// --- Atomic File Operations with Retry ---

// retryOperation retries a function with exponential backoff.
func retryOperation(desc string, op func() error) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		if err = op(); err == nil {
			return nil
		}
		// If dry run, we might get "nil" errors effectively, but logic handles dryRun inside op usually.
		if dryRun {
			return nil
		}
		
		sleepTime := time.Duration(1<<i) * time.Second
		log.Printf("Warning: Failed to %s (attempt %d/%d): %v. Retrying in %s...", desc, i+1, maxRetries, err, sleepTime)
		time.Sleep(sleepTime)
	}
	return fmt.Errorf("failed after %d attempts: %w", maxRetries, err)
}

// copyFileAtomic copies content to a temp file then renames it to overwrite the target.
// This ensures the destination file is never in a partial/corrupted state.
func copyFileAtomic(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	// Ensure destination directory exists
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}

	// Create temp file in the SAME directory as dst to ensure atomic rename works
	tmpDst := dst + ".tmp"
	out, err := os.Create(tmpDst)
	if err != nil {
		return err
	}
	
	// Copy data
	_, err = io.Copy(out, in)
	if err != nil {
		out.Close()
		return err
	}

	// Force write to disk
	if err = out.Sync(); err != nil {
		out.Close()
		return err
	}
	
	// Close before rename
	if err = out.Close(); err != nil {
		return err
	}

	// Atomic Rename
	if err := os.Rename(tmpDst, dst); err != nil {
		return err
	}

	// Attempt to preserve metadata (best effort)
	srcInfo, _ := os.Stat(src)
	if srcInfo != nil {
		os.Chmod(dst, srcInfo.Mode())
		os.Chtimes(dst, time.Now(), srcInfo.ModTime())
	}

	return nil
}

// performSyncAction handles the logic for a single path: decides to Copy, Delete, or Mkdir
// based on the current state of the source.
func performSyncAction(srcPath string) {
	// 1. Check Ignore
	if isPathIgnored(srcPath) {
		return
	}

	destPath, err := getDestPath(srcPath)
	if err != nil {
		log.Printf("Error calculating destination for %s: %v", srcPath, err)
		return
	}

	// 2. Check Source State
	srcInfo, err := os.Stat(srcPath)

	// CASE A: Source Does Not Exist -> Delete Destination
	if os.IsNotExist(err) {
		// Only remove if it exists
		if _, err := os.Stat(destPath); !os.IsNotExist(err) {
			dryRunLog("Deleting: %s", destPath)
			if !dryRun {
				err := retryOperation("remove "+destPath, func() error {
					return os.RemoveAll(destPath)
				})
				if err != nil {
					log.Printf("Error removing %s: %v", destPath, err)
				}
			}
		}
		return
	}

	// CASE B: Source is Directory -> Ensure Dest Dir Exists & Watch
	if srcInfo.IsDir() {
		// Check if directory already exists to avoid verbose logging
		if stat, err := os.Stat(destPath); err == nil && stat.IsDir() {
			// Directory exists. Ensure watcher is active on source (idempotent), but don't log.
			if !dryRun && watcher != nil {
				addWatcherRecursively(watcher, srcPath)
			}
			return
		}

		dryRunLog("Creating Directory: %s", destPath)
		if !dryRun {
			err := retryOperation("mkdir "+destPath, func() error {
				return os.MkdirAll(destPath, srcInfo.Mode())
			})
			if err != nil {
				log.Printf("Error creating directory %s: %v", destPath, err)
			}
			
			// Add to watcher (idempotent usually, but good to ensure)
			if watcher != nil {
				addWatcherRecursively(watcher, srcPath)
			}
		}
		return
	}

	// CASE C: Source is File -> Copy Atomic
	// Check if update is needed
	if areFilesIdenticalRobust(srcPath, destPath) {
		return // Skip
	}

	// Determine specific action verb
	action := "Updating File"
	if _, err := os.Stat(destPath); os.IsNotExist(err) {
		action = "Creating File"
	}

	dryRunLog("%s: %s -> %s", action, srcPath, destPath)
	if !dryRun {
		err := retryOperation("copy "+srcPath, func() error {
			return copyFileAtomic(srcPath, destPath)
		})
		if err != nil {
			log.Printf("Error copying %s: %v", srcPath, err)
		}
	}
}

// --- Initial Sync ---

func initialSync(source, destination string) {
	log.Printf("Starting initial sync (mirror) from %s to %s...", source, destination)

	if !dryRun {
		os.MkdirAll(destination, 0755)
	}

	// 1. Walk Source: Copy/Update
	filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if err != nil { return nil }
		if isPathIgnored(path) {
			if info.IsDir() { return filepath.SkipDir }
			return nil
		}
		if path == source { return nil }

		performSyncAction(path)
		return nil
	})

	// 2. Walk Destination: Delete extras
	filepath.Walk(destination, func(path string, info os.FileInfo, err error) error {
		if err != nil { return nil }
		if path == destination { return nil }

		relPath, _ := filepath.Rel(destination, path)
		sourcePath := filepath.Join(source, relPath)

		// If ignored, do not touch (safety)
		if isPathIgnored(sourcePath) {
			if info.IsDir() { return filepath.SkipDir }
			return nil
		}

		if _, err := os.Stat(sourcePath); os.IsNotExist(err) {
			// Source missing, perform action (which handles deletion)
			performSyncAction(sourcePath) 
			if info.IsDir() { return filepath.SkipDir }
		}
		return nil
	})

	log.Println("Initial sync complete.")
}

// --- Watcher Helpers ---

func addWatcherRecursively(watcher *fsnotify.Watcher, path string) error {
	return filepath.Walk(path, func(walkPath string, info os.FileInfo, err error) error {
		if err != nil { return err }
		if isPathIgnored(walkPath) {
			if info.IsDir() { return filepath.SkipDir }
			return nil
		}
		if info.IsDir() {
			watcher.Add(walkPath)
		}
		return nil
	})
}

// --- Mount Point Check ---
func isMountPoint(path string) (bool, error) {
	absPath, err := filepath.Abs(path)
	if err != nil { return false, err }
	if absPath == "/" { return true, nil }

	var stat, parentStat syscall.Stat_t
	if err := syscall.Stat(absPath, &stat); err != nil {
		return false, fmt.Errorf("stat failed for %s", absPath)
	}
	if err := syscall.Stat(filepath.Dir(absPath), &parentStat); err != nil {
		return false, fmt.Errorf("stat failed for parent of %s", absPath)
	}
	return stat.Dev != parentStat.Dev, nil
}

func main() {
	log.SetFlags(0) // Clean logs

	flag.StringVar(&SOURCE_DIR, "source", "/tmp/efi_source", "Source directory")
	flag.StringVar(&DEST_DIR, "dest", "/tmp/efi_dest", "Destination directory")
	flag.BoolVar(&dryRun, "dry-run", false, "Log actions only")
	flag.Var(&ignoredDirs, "ignore", "Subdirectories to ignore")
	flag.Parse()

	SOURCE_DIR = filepath.Clean(SOURCE_DIR)
	DEST_DIR = filepath.Clean(DEST_DIR)
	
	// Prepare ignored dirs
	for i, dir := range ignoredDirs {
		ignoredDirs[i] = filepath.Clean(filepath.Join(SOURCE_DIR, dir))
	}

	// Validate Mounts
	log.Println("Validating mount points...")
	if ok, _ := isMountPoint(SOURCE_DIR); !ok {
		log.Fatalf("FATAL: Source %s is not a mount point.", SOURCE_DIR)
	}
	if ok, _ := isMountPoint(DEST_DIR); !ok {
		log.Fatalf("FATAL: Destination %s is not a mount point.", DEST_DIR)
	}
	log.Println("Validation successful.")

	// Initial Sync
	initialSync(SOURCE_DIR, DEST_DIR)

	// Watcher Setup
	var err error
	watcher, err = fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	if err := addWatcherRecursively(watcher, SOURCE_DIR); err != nil {
		log.Fatal(err)
	}

	log.Printf("Service started. Watching %s...", SOURCE_DIR)

	// --- Debounce & Signal Handling Loop ---
	
	// Map to store pending events: path -> executionTime
	pendingEvents := make(map[string]time.Time)
	// Ticker to check pending events
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	// Channel for OS signals (Graceful Shutdown)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	done := false
	for !done {
		select {
		// 1. Handle New File Events
		case event, ok := <-watcher.Events:
			if !ok {
				done = true
				break
			}
			// We only care that *something* happened at this path.
			// We rely on performSyncAction to check the actual state (Exist/NotExist) later.
			// This inherently coalesces Create/Write/Chmod into a single action.
			pendingEvents[event.Name] = time.Now().Add(debounceInterval)

		// 2. Handle Watcher Errors
		case err, ok := <-watcher.Errors:
			if !ok {
				done = true
				break
			}
			log.Println("Watcher error:", err)

		// 3. Process Debounced Events
		case <-ticker.C:
			now := time.Now()
			for path, execTime := range pendingEvents {
				if now.After(execTime) {
					// Time to sync!
					delete(pendingEvents, path)
					
					wg.Add(1)
					go func(p string) {
						defer wg.Done()
						performSyncAction(p)
					}(path)
				}
			}

		// 4. Graceful Shutdown
		case <-sigChan:
			log.Println("\nReceived termination signal. Stopping watcher...")
			done = true
		}
	}

	log.Println("Waiting for pending operations to finish...")
	wg.Wait()
	log.Println("Shutdown complete.")
}

