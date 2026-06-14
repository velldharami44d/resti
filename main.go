package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Backend simulates a storage backend with packs and indexes.
type Backend struct {
	mu      sync.Mutex
	packs   map[string]bool
	indexes map[string]IndexData
}

type IndexData struct {
	Packs []string
}

func NewBackend() *Backend {
	return &Backend{
		packs:   make(map[string]bool),
		indexes: make(map[string]IndexData),
	}
}

// Repository manages the repository state and operations.
type Repository struct {
	backend *Backend
}

func NewRepository(b *Backend) *Repository {
	return &Repository{backend: b}
}

// ReconcileIndex detects and removes stale pack references from the index.
func (r *Repository) ReconcileIndex() {
	r.backend.mu.Lock()
	defer r.backend.mu.Unlock()

	fmt.Println("[Reconcile] Scanning for stale pack references in index...")
	for idxID, idxData := range r.backend.indexes {
		var validPacks []string
		staleFound := false
		for _, packID := range idxData.Packs {
			if !r.backend.packs[packID] {
				fmt.Printf("  [Warning] Index %s references non-existent pack %s. Removing stale reference.\n", idxID, packID)
				staleFound = true
			} else {
				validPacks = append(validPacks, packID)
			}
		}
		if staleFound {
			r.backend.indexes[idxID] = IndexData{Packs: validPacks}
		}
	}
}

// Check verifies the integrity of the repository.
func (r *Repository) Check() bool {
	r.backend.mu.Lock()
	defer r.backend.mu.Unlock()

	fmt.Println("[Check] Verifying index and pack alignment...")
	consistent := true
	for idxID, idxData := range r.backend.indexes {
		for _, packID := range idxData.Packs {
			if !r.backend.packs[packID] {
				fmt.Printf("  [Error] Integrity check failed: Index %s points to missing pack %s\n", idxID, packID)
				consistent = false
			}
		}
	}
	if consistent {
		fmt.Println("  [Success] Repository is consistent.")
	} else {
		fmt.Println("  [Failure] Repository is inconsistent. Run prune or rebuild-index to repair.")
	}
	return consistent
}

// Prune runs the prune operation with two-phase commit and signal masking.
func (r *Repository) Prune(ctx context.Context, simulateInterruption bool) error {
	// 1. Reconcile index first to clean up any stale references from previous interrupted runs
	r.ReconcileIndex()

	newPackID := "pack_new_1"
	oldPackID := "pack_old_1"
	oldIndexID := "index_old_1"
	newIndexID := "index_new_1"

	// 2. Write new pack files to backend
	fmt.Println("[Prune] Step 1: Writing new pack files to backend...")
	r.backend.mu.Lock()
	r.backend.packs[newPackID] = true
	r.backend.mu.Unlock()

	// 3. Upload new index files before deleting old packs (Two-Phase Commit)
	fmt.Println("[Prune] Step 2: Uploading new index files...")
	r.backend.mu.Lock()
	r.backend.indexes[newIndexID] = IndexData{Packs: []string{newPackID}}
	r.backend.mu.Unlock()

	if simulateInterruption {
		fmt.Println("[Prune] Simulating interruption/crash before deleting old packs...")
		return fmt.Errorf("interrupted prune operation")
	}

	// 4. Critical Phase - Mask signals and delete old packs/indexes
	fmt.Println("[Prune] Step 3: Entering critical phase. Masking termination signals...")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	defer func() {
		signal.Stop(sigChan)
		fmt.Println("[Prune] Exited critical phase. Signal masking disabled.")
	}()

	// Perform deletions
	fmt.Printf("  Deleting old pack: %s\n", oldPackID)
	r.backend.mu.Lock()
	delete(r.backend.packs, oldPackID)
	r.backend.mu.Unlock()

	// Check if any signals were received during the critical phase
	select {
	case sig := <-sigChan:
		fmt.Printf("  [Signal Masked] Received signal %v during critical phase. Deferring termination.\n", sig)
		defer func() {
			fmt.Printf("  Delivering deferred signal %v now.\n", sig)
			os.Exit(1)
		}()
	default:
	}

	fmt.Printf("  Deleting old index: %s\n", oldIndexID)
	r.backend.mu.Lock()
	delete(r.backend.indexes, oldIndexID)
	r.backend.mu.Unlock()

	fmt.Println("[Prune] Prune operation completed successfully.")
	return nil
}

func main() {
	fmt.Println("=== Starting Restic Prune Simulation ===")
	backend := NewBackend()
	repo := NewRepository(backend)

	// Initialize repository with some data
	backend.packs["pack_old_1"] = true
	backend.indexes["index_old_1"] = IndexData{Packs: []string{"pack_old_1"}}

	ctx := context.Background()

	// Simulate an interrupted prune operation
	fmt.Println("\n--- Running Prune (Interrupted) ---")
	_ = repo.Prune(ctx, true)

	// Check repository state after interruption
	fmt.Println("\n--- Running Check after Interruption ---")
	repo.Check()

	// Run prune again to recover and complete the operation
	fmt.Println("\n--- Running Prune (Recovery) ---")
	_ = repo.Prune(ctx, false)

	// Verify repository is now consistent
	fmt.Println("\n--- Running Check after Recovery ---")
	repo.Check()
}
