package main

import (
	"bytes"
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

const (
	debounceInterval = 500 * time.Millisecond
	maxRetries       = 5
	msdosSuperMagic  = 0x4d44 // Linux statfs magic for FAT/vfat/msdos filesystems.
	tempFilePrefix   = ".esp-sync-"
)

type stringArray []string

func (i *stringArray) String() string {
	return fmt.Sprintf("%v", *i)
}

func (i *stringArray) Set(value string) error {
	if value == "" {
		return nil
	}
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			*i = append(*i, part)
		}
	}
	return nil
}

var (
	SOURCE_DIR     string
	DEST_DIR       string
	dryRun         bool
	resyncInterval time.Duration
	ignoredDirs    stringArray
	watcher        *fsnotify.Watcher
)

func dryRunLog(format string, v ...interface{}) {
	if dryRun {
		log.Printf("[DRY-RUN] "+format, v...)
	} else {
		log.Printf(format, v...)
	}
}

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

func pathContains(parent, child string) bool {
	relPath, err := filepath.Rel(parent, child)
	if err != nil {
		return false
	}
	return relPath != "." && relPath != ".." && !strings.HasPrefix(relPath, ".."+string(filepath.Separator))
}

// areFilesIdenticalRobust compares file contents after a size check.
// FAT32 mtimes and Unix mode bits are not reliable enough for correctness.
func areFilesIdenticalRobust(srcPath, destPath string) bool {
	srcInfo, err := os.Stat(srcPath)
	if err != nil {
		return false
	}
	destInfo, err := os.Stat(destPath)
	if err != nil {
		return false
	}
	if srcInfo.IsDir() || destInfo.IsDir() {
		return false
	}
	if srcInfo.Size() != destInfo.Size() {
		return false
	}

	srcFile, err := os.Open(srcPath)
	if err != nil {
		return false
	}
	defer srcFile.Close()

	destFile, err := os.Open(destPath)
	if err != nil {
		return false
	}
	defer destFile.Close()

	srcBuf := make([]byte, 128*1024)
	destBuf := make([]byte, 128*1024)

	for {
		srcN, srcErr := io.ReadFull(srcFile, srcBuf)
		destN, destErr := io.ReadFull(destFile, destBuf)
		if srcN != destN || !bytes.Equal(srcBuf[:srcN], destBuf[:destN]) {
			return false
		}
		if srcErr == io.EOF || srcErr == io.ErrUnexpectedEOF {
			return destErr == io.EOF || destErr == io.ErrUnexpectedEOF
		}
		if srcErr != nil || destErr != nil {
			return false
		}
	}
}

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

func ensureDirectory(path string, mode os.FileMode) error {
	info, err := os.Stat(path)
	if err == nil {
		if info.IsDir() {
			return nil
		}
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.MkdirAll(path, mode)
}

func syncDirBestEffort(path string) {
	dir, err := os.Open(path)
	if err != nil {
		return
	}
	defer dir.Close()
	_ = dir.Sync()
}

func copyFileAtomic(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	destDir := filepath.Dir(dst)
	if err := ensureDirectory(destDir, 0755); err != nil {
		return err
	}

	out, err := os.CreateTemp(destDir, tempFilePrefix+filepath.Base(dst)+".*.tmp")
	if err != nil {
		return err
	}
	tmpDst := out.Name()
	cleanupTemp := true
	defer func() {
		if cleanupTemp {
			_ = os.Remove(tmpDst)
		}
	}()

	if _, err = io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err = out.Sync(); err != nil {
		out.Close()
		return err
	}
	if err = out.Close(); err != nil {
		return err
	}

	if dstInfo, err := os.Stat(dst); err == nil && dstInfo.IsDir() {
		if err := os.RemoveAll(dst); err != nil {
			return err
		}
	}
	if err := os.Rename(tmpDst, dst); err != nil {
		return err
	}
	cleanupTemp = false

	if srcInfo, err := os.Stat(src); err == nil {
		_ = os.Chtimes(dst, srcInfo.ModTime(), srcInfo.ModTime())
	}
	syncDirBestEffort(destDir)
	return nil
}

func cleanupTempFilesFor(destPath string) {
	destDir := filepath.Dir(destPath)
	basePrefix := tempFilePrefix + filepath.Base(destPath) + "."
	entries, err := os.ReadDir(destDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, basePrefix) && strings.HasSuffix(name, ".tmp") {
			_ = os.Remove(filepath.Join(destDir, name))
		}
	}
}

func performSyncAction(srcPath string) error {
	if isPathIgnored(srcPath) {
		return nil
	}

	destPath, err := getDestPath(srcPath)
	if err != nil {
		return fmt.Errorf("calculating destination for %s: %w", srcPath, err)
	}

	srcInfo, err := os.Stat(srcPath)
	if os.IsNotExist(err) {
		_, dstErr := os.Stat(destPath)
		if !os.IsNotExist(dstErr) {
			dryRunLog("Deleting: %s", destPath)
			if !dryRun {
				err := retryOperation("remove "+destPath, func() error {
					return os.RemoveAll(destPath)
				})
				if err != nil {
					return fmt.Errorf("removing %s: %w", destPath, err)
				}
				syncDirBestEffort(filepath.Dir(destPath))
			}
		}
		if !dryRun {
			cleanupTempFilesFor(destPath)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("stating source %s: %w", srcPath, err)
	}

	if srcInfo.IsDir() {
		if stat, err := os.Stat(destPath); err == nil && stat.IsDir() {
			if !dryRun && watcher != nil {
				if err := addWatcherRecursively(watcher, srcPath); err != nil {
					log.Printf("Warning: Could not watch %s: %v", srcPath, err)
				}
			}
			return nil
		}

		dryRunLog("Creating Directory: %s", destPath)
		if !dryRun {
			err := retryOperation("mkdir "+destPath, func() error {
				return ensureDirectory(destPath, 0755)
			})
			if err != nil {
				return fmt.Errorf("creating directory %s: %w", destPath, err)
			}
			_ = os.Chtimes(destPath, srcInfo.ModTime(), srcInfo.ModTime())
			syncDirBestEffort(filepath.Dir(destPath))
			if watcher != nil {
				if err := addWatcherRecursively(watcher, srcPath); err != nil {
					log.Printf("Warning: Could not watch %s: %v", srcPath, err)
				}
			}
		}
		return nil
	}

	if areFilesIdenticalRobust(srcPath, destPath) {
		return nil
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
			return fmt.Errorf("copying %s: %w", srcPath, err)
		}
	}
	return nil
}

func syncSourceSubtree(path string) error {
	return filepath.Walk(path, func(walkPath string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if isPathIgnored(walkPath) {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		return performSyncAction(walkPath)
	})
}

func reconcileMirror(source, destination string) error {
	log.Printf("Starting mirror reconciliation from %s to %s...", source, destination)

	if !dryRun {
		if err := os.MkdirAll(destination, 0755); err != nil {
			return err
		}
	}

	err := filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if isPathIgnored(path) {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if path == source {
			return nil
		}
		return performSyncAction(path)
	})
	if err != nil {
		return err
	}

	err = filepath.Walk(destination, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if path == destination {
			return nil
		}

		relPath, _ := filepath.Rel(destination, path)
		sourcePath := filepath.Join(source, relPath)

		if isPathIgnored(sourcePath) {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		if _, err := os.Stat(sourcePath); os.IsNotExist(err) {
			if err := performSyncAction(sourcePath); err != nil {
				return err
			}
			if info.IsDir() {
				return filepath.SkipDir
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	log.Println("Mirror reconciliation complete.")
	return nil
}

func addWatcherRecursively(watcher *fsnotify.Watcher, path string) error {
	return filepath.Walk(path, func(walkPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if isPathIgnored(walkPath) {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if info.IsDir() {
			if err := watcher.Add(walkPath); err != nil {
				return err
			}
		}
		return nil
	})
}

func isMountPoint(path string) (bool, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false, err
	}
	if absPath == "/" {
		return true, nil
	}

	var stat, parentStat syscall.Stat_t
	if err := syscall.Stat(absPath, &stat); err != nil {
		return false, fmt.Errorf("stat failed for %s", absPath)
	}
	if err := syscall.Stat(filepath.Dir(absPath), &parentStat); err != nil {
		return false, fmt.Errorf("stat failed for parent of %s", absPath)
	}
	return stat.Dev != parentStat.Dev, nil
}

func isFAT32(path string) (bool, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return false, err
	}
	return uint64(stat.Type) == msdosSuperMagic, nil
}

func validateRootLayout() error {
	sourceAbs, err := filepath.Abs(SOURCE_DIR)
	if err != nil {
		return err
	}
	destAbs, err := filepath.Abs(DEST_DIR)
	if err != nil {
		return err
	}
	sourceAbs = filepath.Clean(sourceAbs)
	destAbs = filepath.Clean(destAbs)

	if sourceAbs == destAbs {
		return fmt.Errorf("source and destination are the same path: %s", sourceAbs)
	}
	if pathContains(sourceAbs, destAbs) {
		return fmt.Errorf("destination %s is inside source %s", destAbs, sourceAbs)
	}
	if pathContains(destAbs, sourceAbs) {
		return fmt.Errorf("source %s is inside destination %s", sourceAbs, destAbs)
	}

	var sourceStat, destStat syscall.Stat_t
	if err := syscall.Stat(sourceAbs, &sourceStat); err != nil {
		return fmt.Errorf("stat failed for source %s: %w", sourceAbs, err)
	}
	if err := syscall.Stat(destAbs, &destStat); err != nil {
		return fmt.Errorf("stat failed for destination %s: %w", destAbs, err)
	}
	if sourceStat.Dev == destStat.Dev {
		return fmt.Errorf("source and destination are on the same filesystem device")
	}
	return nil
}

func validateRoot(path, name string) error {
	ok, err := isMountPoint(path)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%s %s is not a mount point", name, path)
	}
	ok, err = isFAT32(path)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%s %s is not a FAT32/vfat filesystem", name, path)
	}
	return nil
}

func validateSyncRoots() error {
	if err := validateRootLayout(); err != nil {
		return err
	}
	if err := validateRoot(SOURCE_DIR, "source"); err != nil {
		return err
	}
	if err := validateRoot(DEST_DIR, "destination"); err != nil {
		return err
	}
	return nil
}

func runReconcile() {
	if err := validateSyncRoots(); err != nil {
		log.Printf("Skipping sync: %v", err)
		return
	}
	if err := reconcileMirror(SOURCE_DIR, DEST_DIR); err != nil {
		log.Printf("Error during mirror reconciliation: %v", err)
	}
}

func handleSourceEvent(event fsnotify.Event) {
	if err := validateSyncRoots(); err != nil {
		log.Printf("Skipping event for %s: %v", event.Name, err)
		return
	}

	if event.Op&(fsnotify.Remove|fsnotify.Rename) != 0 && watcher != nil {
		_ = watcher.Remove(event.Name)
	}

	if event.Op&(fsnotify.Create|fsnotify.Rename) != 0 {
		if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
			if err := syncSourceSubtree(event.Name); err != nil {
				log.Printf("Error syncing directory subtree %s: %v", event.Name, err)
			}
			return
		}
	}

	if err := performSyncAction(event.Name); err != nil {
		log.Printf("Error syncing %s: %v", event.Name, err)
	}
}

func main() {
	log.SetFlags(0)
	log.SetOutput(os.Stdout)

	flag.StringVar(&SOURCE_DIR, "source", "/tmp/efi_source", "Source directory")
	flag.StringVar(&DEST_DIR, "dest", "/tmp/efi_dest", "Destination directory")
	flag.BoolVar(&dryRun, "dry-run", false, "Log actions only")
	flag.DurationVar(&resyncInterval, "resync-interval", 5*time.Minute, "Periodic full reconciliation interval; 0 disables")
	flag.Var(&ignoredDirs, "ignore", "Subdirectories to ignore")
	flag.Parse()

	var err error
	SOURCE_DIR, err = filepath.Abs(filepath.Clean(SOURCE_DIR))
	if err != nil {
		log.Fatal(err)
	}
	DEST_DIR, err = filepath.Abs(filepath.Clean(DEST_DIR))
	if err != nil {
		log.Fatal(err)
	}

	for i, dir := range ignoredDirs {
		ignoredDirs[i] = filepath.Clean(filepath.Join(SOURCE_DIR, dir))
	}

	log.Println("Validating sync roots...")
	if err := validateSyncRoots(); err != nil {
		log.Fatalf("FATAL: %v.", err)
	}
	log.Println("Validation successful.")

	if err := reconcileMirror(SOURCE_DIR, DEST_DIR); err != nil {
		log.Fatal(err)
	}

	watcher, err = fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	if err := addWatcherRecursively(watcher, SOURCE_DIR); err != nil {
		log.Fatal(err)
	}

	log.Printf("Service started. Watching %s...", SOURCE_DIR)
	if resyncInterval > 0 {
		log.Printf("Periodic reconciliation enabled every %s.", resyncInterval)
	} else {
		log.Println("Periodic reconciliation disabled.")
	}

	pendingEvents := make(map[string]fsnotify.Event)
	pendingTimes := make(map[string]time.Time)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var resyncTicker *time.Ticker
	var resyncChan <-chan time.Time
	if resyncInterval > 0 {
		resyncTicker = time.NewTicker(resyncInterval)
		defer resyncTicker.Stop()
		resyncChan = resyncTicker.C
	}

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
			if existing, ok := pendingEvents[event.Name]; ok {
				event.Op |= existing.Op
			}
			pendingEvents[event.Name] = event
			pendingTimes[event.Name] = time.Now().Add(debounceInterval)

		case err, ok := <-watcher.Errors:
			if !ok {
				done = true
				break
			}
			log.Println("Watcher error:", err)

		case <-ticker.C:
			now := time.Now()
			for path, execTime := range pendingTimes {
				if now.After(execTime) {
					event := pendingEvents[path]
					delete(pendingEvents, path)
					delete(pendingTimes, path)
					handleSourceEvent(event)
				}
			}

		case <-resyncChan:
			runReconcile()

		case <-sigChan:
			log.Println("\nReceived termination signal. Stopping watcher...")
			done = true
		}
	}

	log.Println("Shutdown complete.")
}
