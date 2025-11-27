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
		// Check for the file OR a leftover temp file
		_, dstErr := os.Stat(destPath)
		_, tmpErr := os.Stat(destPath + ".tmp")
		
		exists := !os.IsNotExist(dstErr) || !os.IsNotExist(tmpErr)

		if exists {
			dryRunLog("Deleting: %s", destPath)
			if !dryRun {
				err := retryOperation("remove "+destPath, func() error {
					// Remove main file
					e1 := os.RemoveAll(destPath)
					// Clean up potential temp file from interrupted atomic copy
					e2 := os.RemoveAll(destPath + ".tmp")
					if e1 != nil { return e1 }
					return e2
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
			if watcher != nil {
				addWatcherRecursively(watcher, srcPath)
			}
		}
		return
	}

	// CASE C: Source is File -> Copy Atomic
	if areFilesIdenticalRobust(srcPath, destPath) {
		return // Skip
	}

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

		if isPathIgnored(sourcePath) {
			if info.IsDir() { return filepath.SkipDir }
			return nil
		}

		if _, err := os.Stat(sourcePath); os.IsNotExist(err) {
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
	log.SetFlags(0)
	log.SetOutput(os.Stdout) // Force logs to write to stdout instead of stderr

	flag.StringVar(&SOURCE_DIR, "source", "/tmp/efi_source", "Source directory")
	flag.StringVar(&DEST_DIR, "dest", "/tmp/efi_dest", "Destination directory")
	flag.BoolVar(&dryRun, "dry-run", false, "Log actions only")
	flag.Var(&ignoredDirs, "ignore", "Subdirectories to ignore")
	flag.Parse()

	SOURCE_DIR = filepath.Clean(SOURCE_DIR)
	DEST_DIR = filepath.Clean(DEST_DIR)
	
	for i, dir := range ignoredDirs {
		ignoredDirs[i] = filepath.Clean(filepath.Join(SOURCE_DIR, dir))
	}

	log.Println("Validating mount points...")
	if ok, _ := isMountPoint(SOURCE_DIR); !ok {
		log.Fatalf("FATAL: Source %s is not a mount point.", SOURCE_DIR)
	}
	if ok, _ := isMountPoint(DEST_DIR); !ok {
		log.Fatalf("FATAL: Destination %s is not a mount point.", DEST_DIR)
	}
	log.Println("Validation successful.")

	initialSync(SOURCE_DIR, DEST_DIR)

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

	pendingEvents := make(map[string]time.Time)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	done := false
	for !done {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				done = true
				break
			}
			pendingEvents[event.Name] = time.Now().Add(debounceInterval)

		case err, ok := <-watcher.Errors:
			if !ok {
				done = true
				break
			}
			log.Println("Watcher error:", err)

		case <-ticker.C:
			now := time.Now()
			for path, execTime := range pendingEvents {
				if now.After(execTime) {
					delete(pendingEvents, path)
					
					// IMPORTANT CHANGE: Run synchronously to avoid race conditions 
					// between creating/copying a file and immediately deleting it.
					// On slow USB media, atomic copy+flush+rename can be slow.
					// Running synchronously ensures the file exists before we check if it needs deletion.
					performSyncAction(path)
				}
			}

		case <-sigChan:
			log.Println("\nReceived termination signal. Stopping watcher...")
			done = true
		}
	}

	log.Println("Shutdown complete.")
}

