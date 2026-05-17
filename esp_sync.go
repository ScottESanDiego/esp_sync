// Command esp_sync keeps a destination EFI System Partition mirrored from a
// source EFI System Partition.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"esp_sync/internal/syncer"
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

func main() {
	logger := log.New(os.Stdout, "", 0)

	var ignoredPaths stringArray
	cfg := syncer.Config{
		SourceDir:      "/tmp/efi_source",
		DestDir:        "/tmp/efi_dest",
		ResyncInterval: 5 * time.Minute,
		Logger:         logger,
	}

	flag.StringVar(&cfg.SourceDir, "source", cfg.SourceDir, "Source directory")
	flag.StringVar(&cfg.DestDir, "dest", cfg.DestDir, "Destination directory")
	flag.BoolVar(&cfg.DryRun, "dry-run", false, "Log actions only")
	flag.DurationVar(&cfg.ResyncInterval, "resync-interval", cfg.ResyncInterval, "Periodic full reconciliation interval; 0 disables")
	flag.Var(&ignoredPaths, "ignore", "Subdirectories to ignore")
	flag.Parse()
	cfg.IgnoredPaths = ignoredPaths

	mirror, err := syncer.New(cfg)
	if err != nil {
		logger.Printf("Invalid configuration: %v", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, unix.SIGTERM)
	defer stop()

	if err := mirror.Run(ctx); err != nil {
		logger.Printf("Service stopped with error: %v", err)
		os.Exit(1)
	}
}
