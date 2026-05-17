package syncer

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReconcileMirrorsSourceAndPreservesIgnoredPaths(t *testing.T) {
	t.Parallel()

	source := t.TempDir()
	dest := t.TempDir()
	writeFile(t, source, "EFI/BOOT/BOOTX64.EFI", "new bootloader")
	writeFile(t, source, "EFI/refind/vars/source-var", "source ignored")
	writeFile(t, source, "nested/dir/file.txt", "nested")
	writeFile(t, dest, "EFI/BOOT/BOOTX64.EFI", "old bootloader")
	writeFile(t, dest, "EFI/refind/vars/dest-var", "keep me")
	writeFile(t, dest, "stale/file.txt", "delete me")
	writeFile(t, dest, "stale-root.txt", "delete me too")

	mirror := newTestSyncer(t, source, dest, []string{"EFI/refind/vars"})
	if err := mirror.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	assertFileContent(t, dest, "EFI/BOOT/BOOTX64.EFI", "new bootloader")
	assertFileContent(t, dest, "nested/dir/file.txt", "nested")
	assertFileContent(t, dest, "EFI/refind/vars/dest-var", "keep me")
	assertPathMissing(t, dest, "stale")
	assertPathMissing(t, dest, "stale-root.txt")
}

func TestReconcileReplacesConflictingFileTypes(t *testing.T) {
	t.Parallel()

	source := t.TempDir()
	dest := t.TempDir()
	writeFile(t, source, "dir-replaces-file/child.txt", "child")
	writeFile(t, source, "file-replaces-dir", "file")
	writeFile(t, dest, "dir-replaces-file", "old file")
	writeFile(t, dest, "file-replaces-dir/old-child.txt", "old child")

	mirror := newTestSyncer(t, source, dest, nil)
	if err := mirror.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	assertFileContent(t, dest, "dir-replaces-file/child.txt", "child")
	assertFileContent(t, dest, "file-replaces-dir", "file")
}

func TestSyncSourceSubtreeCopiesPopulatedDirectory(t *testing.T) {
	t.Parallel()

	source := t.TempDir()
	dest := t.TempDir()
	writeFile(t, source, "newdir/child/grandchild.txt", "copied")

	mirror := newTestSyncer(t, source, dest, nil)
	if err := mirror.syncSourceSubtree(context.Background(), filepath.Join(source, "newdir")); err != nil {
		t.Fatalf("syncSourceSubtree() error = %v", err)
	}

	assertFileContent(t, dest, "newdir/child/grandchild.txt", "copied")
}

func TestDryRunDoesNotModifyDestination(t *testing.T) {
	t.Parallel()

	source := t.TempDir()
	dest := t.TempDir()
	writeFile(t, source, "new.txt", "new")
	writeFile(t, dest, "stale.txt", "stale")

	mirror := newTestSyncer(t, source, dest, nil)
	mirror.cfg.DryRun = true
	if err := mirror.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	assertPathMissing(t, dest, "new.txt")
	assertFileContent(t, dest, "stale.txt", "stale")
}

func TestFilesIdenticalUsesContent(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	first := filepath.Join(dir, "first")
	second := filepath.Join(dir, "second")
	if err := os.WriteFile(first, []byte("same-size-a"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(second, []byte("same-size-b"), 0644); err != nil {
		t.Fatal(err)
	}

	if filesIdentical(first, second) {
		t.Fatal("filesIdentical() = true for same-size files with different content")
	}
	if err := os.WriteFile(second, []byte("same-size-a"), 0644); err != nil {
		t.Fatal(err)
	}
	if !filesIdentical(first, second) {
		t.Fatal("filesIdentical() = false for matching files")
	}
}

func TestValidateRootLayoutRejectsDangerousLayouts(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	source := filepath.Join(root, "source")
	dest := filepath.Join(root, "source", "dest")
	if err := os.MkdirAll(dest, 0755); err != nil {
		t.Fatal(err)
	}

	mirror := newTestSyncer(t, source, dest, nil)
	err := mirror.validateRootLayout()
	if err == nil || !strings.Contains(err.Error(), "inside source") {
		t.Fatalf("validateRootLayout() error = %v, want destination-inside-source error", err)
	}
}

func newTestSyncer(t *testing.T, source, dest string, ignored []string) *Syncer {
	t.Helper()

	mirror, err := New(Config{
		SourceDir:        source,
		DestDir:          dest,
		IgnoredPaths:     ignored,
		ResyncInterval:   0,
		DebounceInterval: 10,
		MaxRetries:       1,
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return mirror
}

func writeFile(t *testing.T, root, relPath, content string) {
	t.Helper()

	path := filepath.Join(root, relPath)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}

func assertFileContent(t *testing.T, root, relPath, want string) {
	t.Helper()

	got, err := os.ReadFile(filepath.Join(root, relPath))
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", relPath, err)
	}
	if string(got) != want {
		t.Fatalf("ReadFile(%s) = %q, want %q", relPath, string(got), want)
	}
}

func assertPathMissing(t *testing.T, root, relPath string) {
	t.Helper()

	if _, err := os.Stat(filepath.Join(root, relPath)); !os.IsNotExist(err) {
		t.Fatalf("Stat(%s) error = %v, want not exist", relPath, err)
	}
}
