// Package syncer mirrors one authoritative FAT32 ESP source into one clone
// destination.
package syncer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"golang.org/x/sys/unix"
)

const (
	defaultDebounceInterval = 500 * time.Millisecond
	defaultMaxRetries       = 5
	eventScanInterval       = 100 * time.Millisecond
	tempFilePrefix          = ".esp-sync-"
)

// Config contains the runtime settings for an ESP mirror.
type Config struct {
	// SourceDir is the authoritative ESP mount point.
	SourceDir string
	// DestDir is the clone ESP mount point.
	DestDir string
	// IgnoredPaths are source-relative or absolute paths left unmanaged.
	IgnoredPaths []string
	// DryRun logs intended changes without modifying the destination.
	DryRun bool
	// ResyncInterval controls periodic full reconciliation; zero disables it.
	ResyncInterval time.Duration
	// DebounceInterval delays source events so bursts collapse into one action.
	DebounceInterval time.Duration
	// MaxRetries controls retry attempts for filesystem mutations.
	MaxRetries int
	// Logger receives daemon logs.
	Logger *log.Logger
}

// Syncer mirrors one authoritative source directory into one destination.
type Syncer struct {
	cfg          Config
	sourceDir    string
	destDir      string
	ignoredPaths []string
	logger       *log.Logger
	watcher      *fsnotify.Watcher
}

// New creates a Syncer with normalized absolute paths.
func New(cfg Config) (*Syncer, error) {
	sourceDir, err := filepath.Abs(filepath.Clean(cfg.SourceDir))
	if err != nil {
		return nil, fmt.Errorf("resolve source path: %w", err)
	}
	destDir, err := filepath.Abs(filepath.Clean(cfg.DestDir))
	if err != nil {
		return nil, fmt.Errorf("resolve destination path: %w", err)
	}

	if cfg.DebounceInterval <= 0 {
		cfg.DebounceInterval = defaultDebounceInterval
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = defaultMaxRetries
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}

	ignoredPaths := make([]string, 0, len(cfg.IgnoredPaths))
	for _, ignored := range cfg.IgnoredPaths {
		ignored = filepath.Clean(strings.TrimSpace(ignored))
		if ignored == "." || ignored == "" {
			continue
		}
		if filepath.IsAbs(ignored) {
			ignoredPaths = append(ignoredPaths, ignored)
		} else {
			ignoredPaths = append(ignoredPaths, filepath.Join(sourceDir, ignored))
		}
	}

	return &Syncer{
		cfg:          cfg,
		sourceDir:    sourceDir,
		destDir:      destDir,
		ignoredPaths: ignoredPaths,
		logger:       cfg.Logger,
	}, nil
}

// Run validates the configured roots, performs an initial reconciliation, and
// watches the source until ctx is canceled.
func (s *Syncer) Run(ctx context.Context) error {
	if err := s.ValidateSyncRoots(); err != nil {
		return err
	}
	if err := s.Reconcile(ctx); err != nil {
		return err
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("create watcher: %w", err)
	}
	s.watcher = watcher
	defer func() {
		if err := watcher.Close(); err != nil {
			s.bestEffort("close watcher", "", err)
		}
		s.watcher = nil
	}()

	if err := s.addWatcherRecursively(ctx, watcher, s.sourceDir); err != nil {
		return fmt.Errorf("watch source tree: %w", err)
	}

	s.logger.Printf("Service started. Watching %s...", s.sourceDir)
	if s.cfg.ResyncInterval > 0 {
		s.logger.Printf("Periodic reconciliation enabled every %s.", s.cfg.ResyncInterval)
	} else {
		s.logger.Printf("Periodic reconciliation disabled.")
	}

	pendingEvents := make(map[string]fsnotify.Event)
	pendingTimes := make(map[string]time.Time)
	eventTicker := time.NewTicker(eventScanInterval)
	defer eventTicker.Stop()

	var resyncTicker *time.Ticker
	var resyncChan <-chan time.Time
	if s.cfg.ResyncInterval > 0 {
		resyncTicker = time.NewTicker(s.cfg.ResyncInterval)
		defer resyncTicker.Stop()
		resyncChan = resyncTicker.C
	}

	for {
		select {
		case <-ctx.Done():
			s.logger.Printf("Shutdown requested.")
			return nil

		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			if existing, ok := pendingEvents[event.Name]; ok {
				event.Op |= existing.Op
			}
			pendingEvents[event.Name] = event
			pendingTimes[event.Name] = time.Now().Add(s.cfg.DebounceInterval)

		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			s.logger.Printf("Warning: Watcher error: %v", err)

		case <-eventTicker.C:
			now := time.Now()
			for path, execTime := range pendingTimes {
				if now.Before(execTime) {
					continue
				}
				event := pendingEvents[path]
				delete(pendingEvents, path)
				delete(pendingTimes, path)
				s.handleSourceEvent(ctx, event)
			}

		case <-resyncChan:
			s.runReconcile(ctx)
		}
	}
}

// ValidateSyncRoots verifies that source and destination are separate FAT mount
// points that cannot recursively contain each other.
func (s *Syncer) ValidateSyncRoots() error {
	if err := s.validateRootLayout(); err != nil {
		return err
	}
	if err := validateRoot(s.sourceDir, "source"); err != nil {
		return err
	}
	if err := validateRoot(s.destDir, "destination"); err != nil {
		return err
	}
	return nil
}

// Reconcile mirrors the full source tree into the destination.
func (s *Syncer) Reconcile(ctx context.Context) error {
	s.logger.Printf("Starting mirror reconciliation from %s to %s...", s.sourceDir, s.destDir)

	if !s.cfg.DryRun {
		if err := os.MkdirAll(s.destDir, 0755); err != nil {
			return fmt.Errorf("create destination root: %w", err)
		}
	}

	if err := s.walkSource(ctx, s.sourceDir); err != nil {
		return err
	}
	if err := s.deleteDestinationExtras(ctx); err != nil {
		return err
	}

	s.logger.Printf("Mirror reconciliation complete.")
	return nil
}

func (s *Syncer) walkSource(ctx context.Context, root string) error {
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			s.bestEffort("walk source", path, walkErr)
			return nil
		}
		if s.isPathIgnored(path) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if path == s.sourceDir {
			return nil
		}
		return s.performSyncAction(ctx, path)
	})
}

func (s *Syncer) deleteDestinationExtras(ctx context.Context) error {
	return filepath.WalkDir(s.destDir, func(path string, entry fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			s.bestEffort("walk destination", path, walkErr)
			return nil
		}
		if path == s.destDir {
			return nil
		}

		relPath, err := filepath.Rel(s.destDir, path)
		if err != nil {
			return fmt.Errorf("relative destination path: %w", err)
		}
		sourcePath := filepath.Join(s.sourceDir, relPath)

		if s.isPathIgnored(sourcePath) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		if _, err := os.Stat(sourcePath); errors.Is(err, os.ErrNotExist) {
			if err := s.performSyncAction(ctx, sourcePath); err != nil {
				return err
			}
			if entry.IsDir() {
				return filepath.SkipDir
			}
		}
		return nil
	})
}

func (s *Syncer) syncSourceSubtree(ctx context.Context, path string) error {
	return filepath.WalkDir(path, func(walkPath string, entry fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			s.bestEffort("walk source subtree", walkPath, walkErr)
			return nil
		}
		if s.isPathIgnored(walkPath) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		return s.performSyncAction(ctx, walkPath)
	})
}

func (s *Syncer) performSyncAction(ctx context.Context, srcPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.isPathIgnored(srcPath) {
		return nil
	}

	destPath, err := s.destPath(srcPath)
	if err != nil {
		return err
	}

	srcInfo, err := os.Stat(srcPath)
	if errors.Is(err, os.ErrNotExist) {
		return s.deleteDestinationPath(ctx, destPath)
	}
	if err != nil {
		return fmt.Errorf("stat source %s: %w", srcPath, err)
	}

	if srcInfo.IsDir() {
		return s.ensureDestinationDirectory(ctx, srcPath, destPath, srcInfo.ModTime())
	}
	return s.copyDestinationFile(ctx, srcPath, destPath)
}

func (s *Syncer) deleteDestinationPath(ctx context.Context, destPath string) error {
	_, err := os.Stat(destPath)
	if err == nil {
		s.logAction("Deleting: %s", destPath)
		if !s.cfg.DryRun {
			if err := s.retryOperation(ctx, "remove "+destPath, func() error {
				return os.RemoveAll(destPath)
			}); err != nil {
				return fmt.Errorf("remove %s: %w", destPath, err)
			}
			s.syncDirBestEffort(filepath.Dir(destPath))
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat destination %s: %w", destPath, err)
	}

	if !s.cfg.DryRun {
		s.cleanupTempFilesFor(destPath)
	}
	return nil
}

func (s *Syncer) ensureDestinationDirectory(ctx context.Context, srcPath, destPath string, modTime time.Time) error {
	if stat, err := os.Stat(destPath); err == nil && stat.IsDir() {
		if !s.cfg.DryRun && s.watcher != nil {
			if err := s.addWatcherRecursively(ctx, s.watcher, srcPath); err != nil {
				s.bestEffort("watch source directory", srcPath, err)
			}
		}
		return nil
	}

	s.logAction("Creating Directory: %s", destPath)
	if s.cfg.DryRun {
		return nil
	}
	if err := s.retryOperation(ctx, "mkdir "+destPath, func() error {
		return ensureDirectory(destPath, 0755)
	}); err != nil {
		return fmt.Errorf("create directory %s: %w", destPath, err)
	}
	s.chtimesBestEffort(destPath, modTime)
	s.syncDirBestEffort(filepath.Dir(destPath))
	if s.watcher != nil {
		if err := s.addWatcherRecursively(ctx, s.watcher, srcPath); err != nil {
			s.bestEffort("watch source directory", srcPath, err)
		}
	}
	return nil
}

func (s *Syncer) copyDestinationFile(ctx context.Context, srcPath, destPath string) error {
	if filesIdentical(srcPath, destPath) {
		return nil
	}

	action := "Updating File"
	if _, err := os.Stat(destPath); errors.Is(err, os.ErrNotExist) {
		action = "Creating File"
	}
	s.logAction("%s: %s -> %s", action, srcPath, destPath)

	if s.cfg.DryRun {
		return nil
	}
	if err := s.retryOperation(ctx, "copy "+srcPath, func() error {
		return s.copyFileAtomic(srcPath, destPath)
	}); err != nil {
		return fmt.Errorf("copy %s: %w", srcPath, err)
	}
	return nil
}

func (s *Syncer) retryOperation(ctx context.Context, desc string, op func() error) error {
	var err error
	for i := 0; i < s.cfg.MaxRetries; i++ {
		if err = ctx.Err(); err != nil {
			return err
		}
		if err = op(); err == nil {
			return nil
		}
		if i == s.cfg.MaxRetries-1 {
			break
		}

		sleepTime := time.Duration(1<<i) * time.Second
		s.logger.Printf("Warning: Failed to %s (attempt %d/%d): %v. Retrying in %s...", desc, i+1, s.cfg.MaxRetries, err, sleepTime)

		timer := time.NewTimer(sleepTime)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
	return fmt.Errorf("failed after %d attempts: %w", s.cfg.MaxRetries, err)
}

func (s *Syncer) copyFileAtomic(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() {
		if err := in.Close(); err != nil {
			s.bestEffort("close source file", src, err)
		}
	}()

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
			s.removeBestEffort(tmpDst)
		}
	}()

	if _, err = io.Copy(out, in); err != nil {
		s.closeBestEffort(out, tmpDst)
		return err
	}
	if err = out.Sync(); err != nil {
		s.closeBestEffort(out, tmpDst)
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
		s.chtimesBestEffort(dst, srcInfo.ModTime())
	} else {
		s.bestEffort("stat copied source", src, err)
	}
	s.syncDirBestEffort(destDir)
	return nil
}

func (s *Syncer) handleSourceEvent(ctx context.Context, event fsnotify.Event) {
	if err := s.ValidateSyncRoots(); err != nil {
		s.logger.Printf("Warning: Skipping event for %s because sync roots are invalid: %v", event.Name, err)
		return
	}

	if event.Op&(fsnotify.Remove|fsnotify.Rename) != 0 && s.watcher != nil {
		if err := s.watcher.Remove(event.Name); err != nil {
			s.bestEffort("remove watcher", event.Name, err)
		}
	}

	if event.Op&(fsnotify.Create|fsnotify.Rename) != 0 {
		if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
			if err := s.syncSourceSubtree(ctx, event.Name); err != nil {
				s.logger.Printf("Error syncing directory subtree %s: %v", event.Name, err)
			}
			return
		}
	}

	if err := s.performSyncAction(ctx, event.Name); err != nil {
		s.logger.Printf("Error syncing %s: %v", event.Name, err)
	}
}

func (s *Syncer) runReconcile(ctx context.Context) {
	if err := s.ValidateSyncRoots(); err != nil {
		s.logger.Printf("Warning: Skipping sync because sync roots are invalid: %v", err)
		return
	}
	if err := s.Reconcile(ctx); err != nil {
		s.logger.Printf("Error during mirror reconciliation: %v", err)
	}
}

func (s *Syncer) addWatcherRecursively(ctx context.Context, watcher *fsnotify.Watcher, path string) error {
	return filepath.WalkDir(path, func(walkPath string, entry fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			return walkErr
		}
		if s.isPathIgnored(walkPath) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			if err := watcher.Add(walkPath); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Syncer) destPath(srcPath string) (string, error) {
	relPath, err := filepath.Rel(s.sourceDir, srcPath)
	if err != nil {
		return "", fmt.Errorf("relative source path: %w", err)
	}
	return filepath.Join(s.destDir, relPath), nil
}

func (s *Syncer) isPathIgnored(path string) bool {
	cleanPath := filepath.Clean(path)
	for _, ignoredPath := range s.ignoredPaths {
		if cleanPath == ignoredPath {
			return true
		}
		if strings.HasPrefix(cleanPath, ignoredPath+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

func (s *Syncer) validateRootLayout() error {
	if s.sourceDir == s.destDir {
		return fmt.Errorf("source and destination are the same path: %s", s.sourceDir)
	}
	if pathContains(s.sourceDir, s.destDir) {
		return fmt.Errorf("destination %s is inside source %s", s.destDir, s.sourceDir)
	}
	if pathContains(s.destDir, s.sourceDir) {
		return fmt.Errorf("source %s is inside destination %s", s.sourceDir, s.destDir)
	}

	var sourceStat, destStat unix.Stat_t
	if err := unix.Stat(s.sourceDir, &sourceStat); err != nil {
		return fmt.Errorf("stat source %s: %w", s.sourceDir, err)
	}
	if err := unix.Stat(s.destDir, &destStat); err != nil {
		return fmt.Errorf("stat destination %s: %w", s.destDir, err)
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
	ok, err = isFAT(path)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%s %s is not a FAT32/vfat filesystem", name, path)
	}
	return nil
}

func isMountPoint(path string) (bool, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false, err
	}
	if absPath == "/" {
		return true, nil
	}

	var stat, parentStat unix.Stat_t
	if err := unix.Stat(absPath, &stat); err != nil {
		return false, fmt.Errorf("stat %s: %w", absPath, err)
	}
	if err := unix.Stat(filepath.Dir(absPath), &parentStat); err != nil {
		return false, fmt.Errorf("stat parent of %s: %w", absPath, err)
	}
	return stat.Dev != parentStat.Dev, nil
}

func isFAT(path string) (bool, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return false, err
	}
	return uint64(stat.Type) == unix.MSDOS_SUPER_MAGIC, nil
}

func pathContains(parent, child string) bool {
	relPath, err := filepath.Rel(parent, child)
	if err != nil {
		return false
	}
	return relPath != "." && relPath != ".." && !strings.HasPrefix(relPath, ".."+string(filepath.Separator))
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
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.MkdirAll(path, mode)
}

func filesIdentical(srcPath, destPath string) bool {
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

func (s *Syncer) cleanupTempFilesFor(destPath string) {
	destDir := filepath.Dir(destPath)
	basePrefix := tempFilePrefix + filepath.Base(destPath) + "."
	entries, err := os.ReadDir(destDir)
	if err != nil {
		s.bestEffort("read destination directory for temp cleanup", destDir, err)
		return
	}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, basePrefix) && strings.HasSuffix(name, ".tmp") {
			s.removeBestEffort(filepath.Join(destDir, name))
		}
	}
}

func (s *Syncer) syncDirBestEffort(path string) {
	dir, err := os.Open(path)
	if err != nil {
		s.bestEffort("open directory for sync", path, err)
		return
	}
	defer func() {
		if err := dir.Close(); err != nil {
			s.bestEffort("close directory", path, err)
		}
	}()
	if err := dir.Sync(); err != nil {
		s.bestEffort("sync directory", path, err)
	}
}

func (s *Syncer) chtimesBestEffort(path string, modTime time.Time) {
	if err := os.Chtimes(path, modTime, modTime); err != nil {
		s.bestEffort("preserve modification time", path, err)
	}
}

func (s *Syncer) removeBestEffort(path string) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		s.bestEffort("remove temporary file", path, err)
	}
}

func (s *Syncer) closeBestEffort(file *os.File, path string) {
	if err := file.Close(); err != nil {
		s.bestEffort("close file", path, err)
	}
}

func (s *Syncer) bestEffort(_, _ string, _ error) {
	// Best-effort filesystem cleanup and metadata operations intentionally do
	// not emit normal logs; many filesystems reject some of them harmlessly.
}

func (s *Syncer) logAction(format string, args ...any) {
	message := fmt.Sprintf(format, args...)
	if s.cfg.DryRun {
		message = "[DRY-RUN] " + message
	}
	s.logger.Print(message)
}
