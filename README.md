# esp_sync

`esp_sync` keeps one EFI System Partition clone mirrored from one authoritative
source EFI System Partition.

It is a one-way synchronizer: changes under `--source` are copied to `--dest`.
Changes made directly under `--dest` are treated as drift and may be overwritten
or deleted during reconciliation.

The program is intended for Linux systems where both ESPs are mounted as FAT32
(`vfat`) filesystems.

## What It Does

- Verifies that source and destination are separate mount points.
- Verifies that both mounts are FAT/vfat filesystems.
- Rejects source/destination paths that are the same, nested inside each other,
  or on the same filesystem device.
- Performs a full mirror reconciliation at startup.
- Watches the source tree with `fsnotify` and applies changes to the clone.
- Periodically performs a full reconciliation to recover from missed events.
- Copies files by writing a temporary file in the destination directory, syncing
  it, then renaming it into place.
- Deletes clone files and directories that do not exist in the source.
- Preserves file and directory modification times on a best-effort basis.
- Supports ignored source-relative paths.

## What It Does Not Do

- It does not mount partitions.
- It does not sync bidirectionally.
- It does not merge changes.
- It does not prove that a mounted FAT32 filesystem has the GPT EFI System
  Partition type; it validates the active mount point and filesystem type.

## Build

```sh
go build -o esp_sync .
```

## Usage

```sh
sudo ./esp_sync --source /efi --dest /efi2
```

Common options:

```sh
sudo ./esp_sync \
  --source /efi \
  --dest /efi2 \
  --ignore EFI/refind/vars \
  --resync-interval 5m
```

Flags:

- `--source`: authoritative ESP mount point.
- `--dest`: clone ESP mount point.
- `--ignore`: comma-separated source-relative paths to leave unmanaged. May be
  passed more than once.
- `--resync-interval`: interval for full reconciliation. Defaults to `5m`.
  Use `0` to disable periodic reconciliation.
- `--dry-run`: log intended actions without changing the destination.

Ignored paths are interpreted relative to `--source`. For example:

```sh
--ignore EFI/refind/vars
```

ignores `/efi/EFI/refind/vars` when `--source /efi` is used. Ignored destination
paths are not deleted during reconciliation.

## OpenRC

Example configuration is provided in `init-scripts/openrc`.

`init-scripts/openrc/esp_sync.confd`:

```sh
ESP_SOURCE="/efi"
ESP_DEST="/efi2"
ESP_EXTRAARGS="--ignore EFI/refind/vars"
```

The init script invokes:

```sh
/usr/bin/esp_sync --source "${ESP_SOURCE}" --dest "${ESP_DEST}" ${ESP_EXTRAARGS}
```

## Operational Notes

Mount both ESPs before starting the daemon. If either mount disappears while the
daemon is running, event handling and periodic reconciliation are skipped until
the validation checks pass again.

Because FAT32 timestamp and permission behavior differs from Unix filesystems,
`esp_sync` uses size plus byte-for-byte content comparison for file correctness.
Unix ownership and permissions are not used as synchronization criteria.
