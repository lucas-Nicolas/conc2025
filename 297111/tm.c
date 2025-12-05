/**
 * @file   tm.c
 * @author [...]
 *
 * @section LICENSE
 *
 * [...]
 *
 * @section DESCRIPTION
 *
 * Implementation of a dual-versioned batched software transactional memory.
 **/

// Requested features
#define _GNU_SOURCE
#define _POSIX_C_SOURCE   200809L
#ifdef __STDC_NO_ATOMICS__
    #error Current C11 compiler does not support atomic operations
#endif

// External headers
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <pthread.h>

// Internal headers
#include <tm.h>
#include "macros.h"

// ============================================================================
// Data Structures
// ============================================================================

// Per-word control: stores tx_id of writer (0 means not written this epoch)
typedef atomic_uint_fast32_t word_ctrl_t;

#define CTRL_NONE       0U

// Segment structure
typedef struct segment {
    void*        user_base;     // User-visible base pointer
    size_t       size;          // Size in bytes
    size_t       word_count;    // Number of words (size / align)
    word_ctrl_t* ctrl;          // Per-word metadata
    void*        copy_a;        // Data copy A
    void*        copy_b;        // Data copy B
    atomic_uint_fast8_t readable_copy;  // 0: A is readable, 1: B is readable
    struct segment* next;       // Linked list for segment registry
} segment_t;

// Batcher for epoch management
typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t  cond;
    uint64_t        epoch;       // Current epoch
    size_t          remaining;   // Tx remaining in current epoch
    size_t          blocked;     // Tx waiting for next epoch
} batcher_t;

// Write-set entry
typedef struct {
    segment_t* seg;
    size_t     word_idx;
} write_entry_t;

// Forward declarations
typedef struct region region_t;

// Shared region structure
struct region {
    size_t              align;
    size_t              first_seg_size;
    segment_t*          first_segment;    // Non-freeable first segment
    segment_t*          segments;         // Linked list of all segments
    pthread_mutex_t     seg_lock;         // Lock for segment registry
    batcher_t           batcher;
    atomic_uint_fast32_t next_tx_id;      // Atomic counter for tx IDs (starts at 1)

    // Per-epoch data for commit
    write_entry_t*      epoch_writes;
    size_t              epoch_write_count;
    size_t              epoch_write_cap;
    pthread_mutex_t     epoch_lock;       // Lock for epoch data

    // Deferred frees
    segment_t**         epoch_frees;
    size_t              epoch_free_count;
    size_t              epoch_free_cap;
};

// Transaction structure
typedef struct {
    region_t*       region;
    bool            is_ro;
    uint32_t        tx_id;
    bool            aborted;

    // Write set for this transaction (for rollback on abort and commit)
    write_entry_t*  writes;
    size_t          write_count;
    size_t          write_cap;

    // Allocated segments (for rollback on abort)
    segment_t**     allocs;
    size_t          alloc_count;
    size_t          alloc_cap;

    // Segments to free on commit
    segment_t**     frees;
    size_t          free_count;
    size_t          free_cap;
} tx_internal_t;

// ============================================================================
// Batcher Implementation
// ============================================================================

static bool batcher_init(batcher_t* b) {
    if (pthread_mutex_init(&b->mutex, NULL) != 0) return false;
    if (pthread_cond_init(&b->cond, NULL) != 0) {
        pthread_mutex_destroy(&b->mutex);
        return false;
    }
    b->epoch = 0;
    b->remaining = 0;
    b->blocked = 0;
    return true;
}

static void batcher_destroy(batcher_t* b) {
    pthread_mutex_destroy(&b->mutex);
    pthread_cond_destroy(&b->cond);
}

static uint64_t batcher_enter(batcher_t* b) {
    pthread_mutex_lock(&b->mutex);
    if (b->remaining == 0) {
        // First in a new epoch
        b->remaining = 1;
    } else {
        // Join current epoch
        b->remaining++;
    }
    uint64_t epoch = b->epoch;
    pthread_mutex_unlock(&b->mutex);
    return epoch;
}

// Forward declaration
static void apply_epoch_changes(region_t* region);

static void batcher_leave(batcher_t* b, region_t* region) {
    pthread_mutex_lock(&b->mutex);
    b->remaining--;
    if (b->remaining == 0) {
        // Last transaction in epoch - apply changes
        apply_epoch_changes(region);
        // Move to next epoch
        b->epoch++;
    }
    pthread_mutex_unlock(&b->mutex);
}

// ============================================================================
// Helper Functions
// ============================================================================

// Create a new segment
static segment_t* segment_create(size_t size, size_t align) {
    segment_t* seg = (segment_t*)malloc(sizeof(segment_t));
    if (!seg) return NULL;

    size_t word_count = size / align;

    // Allocate aligned user base
    if (posix_memalign(&seg->user_base, align, size) != 0) {
        free(seg);
        return NULL;
    }

    // Allocate aligned data copies
    if (posix_memalign(&seg->copy_a, align, size) != 0) {
        free(seg->user_base);
        free(seg);
        return NULL;
    }
    if (posix_memalign(&seg->copy_b, align, size) != 0) {
        free(seg->copy_a);
        free(seg->user_base);
        free(seg);
        return NULL;
    }

    // Allocate control array
    seg->ctrl = (word_ctrl_t*)calloc(word_count, sizeof(word_ctrl_t));
    if (!seg->ctrl) {
        free(seg->copy_b);
        free(seg->copy_a);
        free(seg->user_base);
        free(seg);
        return NULL;
    }

    // Initialize
    seg->size = size;
    seg->word_count = word_count;
    seg->next = NULL;
    atomic_store(&seg->readable_copy, 0);  // A is readable initially

    // Initialize both copies to zero
    memset(seg->copy_a, 0, size);
    memset(seg->copy_b, 0, size);

    return seg;
}

// Destroy a segment
static void segment_destroy(segment_t* seg) {
    if (!seg) return;
    free(seg->ctrl);
    free(seg->copy_b);
    free(seg->copy_a);
    free(seg->user_base);
    free(seg);
}

// Find segment containing pointer (no locking)
static segment_t* find_segment_unlocked(region_t* region, const void* ptr) {
    segment_t* seg = region->segments;
    while (seg) {
        const uint8_t* base = (const uint8_t*)seg->user_base;
        const uint8_t* p = (const uint8_t*)ptr;
        if (p >= base && p < base + seg->size) {
            return seg;
        }
        seg = seg->next;
    }
    return NULL;
}

// Get readable copy pointer for a word
static inline void* get_readable_copy(segment_t* seg, size_t word_idx, size_t align) {
    uint8_t readable = atomic_load(&seg->readable_copy);
    void* base = (readable == 0) ? seg->copy_a : seg->copy_b;
    return (uint8_t*)base + word_idx * align;
}

// Get writable copy pointer for a word
static inline void* get_writable_copy(segment_t* seg, size_t word_idx, size_t align) {
    uint8_t readable = atomic_load(&seg->readable_copy);
    void* base = (readable == 0) ? seg->copy_b : seg->copy_a;  // Opposite of readable
    return (uint8_t*)base + word_idx * align;
}

// Apply epoch changes (called when last tx leaves - batcher mutex is held)
static void apply_epoch_changes(region_t* region) {
    pthread_mutex_lock(&region->epoch_lock);

    // Copy writable to readable for all committed writes and reset ctrl
    for (size_t i = 0; i < region->epoch_write_count; i++) {
        segment_t* seg = region->epoch_writes[i].seg;
        size_t word_idx = region->epoch_writes[i].word_idx;
        size_t align = region->align;

        // Only copy if the write actually committed (ctrl != 0)
        uint32_t ctrl = atomic_load(&seg->ctrl[word_idx]);
        if (ctrl != CTRL_NONE) {
            // Copy writable to readable
            void* readable = get_readable_copy(seg, word_idx, align);
            void* writable = get_writable_copy(seg, word_idx, align);
            memcpy(readable, writable, align);

            // Reset control
            atomic_store(&seg->ctrl[word_idx], CTRL_NONE);
        }
    }
    region->epoch_write_count = 0;

    // Perform deferred frees
    pthread_mutex_lock(&region->seg_lock);
    for (size_t i = 0; i < region->epoch_free_count; i++) {
        segment_t* seg_to_free = region->epoch_frees[i];

        // Remove from linked list
        segment_t** pp = &region->segments;
        while (*pp) {
            if (*pp == seg_to_free) {
                *pp = seg_to_free->next;
                break;
            }
            pp = &(*pp)->next;
        }

        segment_destroy(seg_to_free);
    }
    region->epoch_free_count = 0;
    pthread_mutex_unlock(&region->seg_lock);

    pthread_mutex_unlock(&region->epoch_lock);
}

// Add write entries to epoch writes (called at commit time)
static bool add_epoch_writes_batch(region_t* region, write_entry_t* writes, size_t count) {
    if (count == 0) return true;

    pthread_mutex_lock(&region->epoch_lock);

    size_t needed = region->epoch_write_count + count;
    if (needed > region->epoch_write_cap) {
        size_t new_cap = region->epoch_write_cap == 0 ? 256 : region->epoch_write_cap;
        while (new_cap < needed) new_cap *= 2;
        write_entry_t* new_writes = (write_entry_t*)realloc(region->epoch_writes,
                                                             new_cap * sizeof(write_entry_t));
        if (!new_writes) {
            pthread_mutex_unlock(&region->epoch_lock);
            return false;
        }
        region->epoch_writes = new_writes;
        region->epoch_write_cap = new_cap;
    }

    memcpy(&region->epoch_writes[region->epoch_write_count], writes, count * sizeof(write_entry_t));
    region->epoch_write_count += count;

    pthread_mutex_unlock(&region->epoch_lock);
    return true;
}

// Add segment to epoch frees
static bool add_epoch_free(region_t* region, segment_t* seg) {
    pthread_mutex_lock(&region->epoch_lock);

    if (region->epoch_free_count >= region->epoch_free_cap) {
        size_t new_cap = region->epoch_free_cap == 0 ? 16 : region->epoch_free_cap * 2;
        segment_t** new_frees = (segment_t**)realloc(region->epoch_frees,
                                                      new_cap * sizeof(segment_t*));
        if (!new_frees) {
            pthread_mutex_unlock(&region->epoch_lock);
            return false;
        }
        region->epoch_frees = new_frees;
        region->epoch_free_cap = new_cap;
    }

    region->epoch_frees[region->epoch_free_count++] = seg;

    pthread_mutex_unlock(&region->epoch_lock);
    return true;
}

// ============================================================================
// Transaction Helper Functions
// ============================================================================

static bool tx_add_write(tx_internal_t* tx, segment_t* seg, size_t word_idx) {
    if (tx->write_count >= tx->write_cap) {
        size_t new_cap = tx->write_cap == 0 ? 64 : tx->write_cap * 2;
        write_entry_t* new_writes = (write_entry_t*)realloc(tx->writes,
                                                             new_cap * sizeof(write_entry_t));
        if (!new_writes) return false;
        tx->writes = new_writes;
        tx->write_cap = new_cap;
    }
    tx->writes[tx->write_count].seg = seg;
    tx->writes[tx->write_count].word_idx = word_idx;
    tx->write_count++;
    return true;
}

static bool tx_add_alloc(tx_internal_t* tx, segment_t* seg) {
    if (tx->alloc_count >= tx->alloc_cap) {
        size_t new_cap = tx->alloc_cap == 0 ? 8 : tx->alloc_cap * 2;
        segment_t** new_allocs = (segment_t**)realloc(tx->allocs,
                                                       new_cap * sizeof(segment_t*));
        if (!new_allocs) return false;
        tx->allocs = new_allocs;
        tx->alloc_cap = new_cap;
    }
    tx->allocs[tx->alloc_count++] = seg;
    return true;
}

static bool tx_add_free(tx_internal_t* tx, segment_t* seg) {
    if (tx->free_count >= tx->free_cap) {
        size_t new_cap = tx->free_cap == 0 ? 8 : tx->free_cap * 2;
        segment_t** new_frees = (segment_t**)realloc(tx->frees,
                                                      new_cap * sizeof(segment_t*));
        if (!new_frees) return false;
        tx->frees = new_frees;
        tx->free_cap = new_cap;
    }
    tx->frees[tx->free_count++] = seg;
    return true;
}

// Cleanup and leave batcher (called on abort or at end)
static void tx_cleanup(tx_internal_t* tx, bool committed) {
    region_t* region = tx->region;

    if (!committed && !tx->is_ro) {
        // On abort: clear word metadata for words we wrote
        for (size_t i = 0; i < tx->write_count; i++) {
            segment_t* seg = tx->writes[i].seg;
            size_t word_idx = tx->writes[i].word_idx;

            // Only clear if we still own it (CAS to avoid races)
            uint32_t expected = tx->tx_id;
            atomic_compare_exchange_strong(&seg->ctrl[word_idx], &expected, CTRL_NONE);
        }

        // On abort: remove and destroy allocated segments
        pthread_mutex_lock(&region->seg_lock);
        for (size_t i = 0; i < tx->alloc_count; i++) {
            segment_t* seg = tx->allocs[i];
            segment_t** pp = &region->segments;
            while (*pp) {
                if (*pp == seg) {
                    *pp = seg->next;
                    break;
                }
                pp = &(*pp)->next;
            }
        }
        pthread_mutex_unlock(&region->seg_lock);

        // Now destroy outside the lock
        for (size_t i = 0; i < tx->alloc_count; i++) {
            segment_destroy(tx->allocs[i]);
        }
    }

    // Leave batcher
    batcher_leave(&region->batcher, region);
}

static void tx_destroy(tx_internal_t* tx) {
    free(tx->writes);
    free(tx->allocs);
    free(tx->frees);
    free(tx);
}

// ============================================================================
// Read/Write Word Logic
// ============================================================================

// Read a single word
static bool read_word(tx_internal_t* tx, segment_t* seg, size_t word_idx, void* target) {
    size_t align = tx->region->align;

    if (tx->is_ro) {
        // Read-only transaction: always read from readable copy
        void* src = get_readable_copy(seg, word_idx, align);
        memcpy(target, src, align);
        return true;
    }

    // Read-write transaction
    uint32_t owner = atomic_load(&seg->ctrl[word_idx]);

    if (owner == CTRL_NONE) {
        // Not written this epoch - read from readable copy
        void* src = get_readable_copy(seg, word_idx, align);
        memcpy(target, src, align);
        return true;
    } else if (owner == tx->tx_id) {
        // Written by us - read from writable copy
        void* src = get_writable_copy(seg, word_idx, align);
        memcpy(target, src, align);
        return true;
    } else {
        // Written by another tx - conflict
        return false;
    }
}

// Write a single word
static bool write_word(tx_internal_t* tx, segment_t* seg, size_t word_idx, const void* source) {
    size_t align = tx->region->align;

    // Try to acquire ownership: NONE -> tx_id
    uint32_t expected = CTRL_NONE;
    if (atomic_compare_exchange_strong(&seg->ctrl[word_idx], &expected, tx->tx_id)) {
        // Success! We now own this word
        void* dst = get_writable_copy(seg, word_idx, align);
        memcpy(dst, source, align);

        // Add to local write set (will be registered at commit time)
        if (!tx_add_write(tx, seg, word_idx)) {
            return false;
        }

        return true;
    }

    // CAS failed - check who owns it
    uint32_t owner = expected;  // CAS returns current value in expected on failure

    if (owner == tx->tx_id) {
        // We already own it - just write
        void* dst = get_writable_copy(seg, word_idx, align);
        memcpy(dst, source, align);
        return true;
    } else {
        // Another tx owns it - conflict
        return false;
    }
}

// ============================================================================
// TM API Implementation
// ============================================================================

shared_t tm_create(size_t size, size_t align) {
    region_t* region = (region_t*)malloc(sizeof(region_t));
    if (!region) return invalid_shared;

    region->align = align;
    region->first_seg_size = size;
    region->segments = NULL;
    region->epoch_writes = NULL;
    region->epoch_write_count = 0;
    region->epoch_write_cap = 0;
    region->epoch_frees = NULL;
    region->epoch_free_count = 0;
    region->epoch_free_cap = 0;
    atomic_store(&region->next_tx_id, 1);  // Start at 1

    if (pthread_mutex_init(&region->seg_lock, NULL) != 0) {
        free(region);
        return invalid_shared;
    }

    if (pthread_mutex_init(&region->epoch_lock, NULL) != 0) {
        pthread_mutex_destroy(&region->seg_lock);
        free(region);
        return invalid_shared;
    }

    if (!batcher_init(&region->batcher)) {
        pthread_mutex_destroy(&region->epoch_lock);
        pthread_mutex_destroy(&region->seg_lock);
        free(region);
        return invalid_shared;
    }

    // Create first segment
    segment_t* first = segment_create(size, align);
    if (!first) {
        batcher_destroy(&region->batcher);
        pthread_mutex_destroy(&region->epoch_lock);
        pthread_mutex_destroy(&region->seg_lock);
        free(region);
        return invalid_shared;
    }

    region->first_segment = first;
    region->segments = first;

    return (shared_t)region;
}

void tm_destroy(shared_t shared) {
    region_t* region = (region_t*)shared;
    if (!region) return;

    // Free all segments
    segment_t* seg = region->segments;
    while (seg) {
        segment_t* next = seg->next;
        segment_destroy(seg);
        seg = next;
    }

    // Free epoch data
    free(region->epoch_writes);
    free(region->epoch_frees);

    // Destroy synchronization primitives
    batcher_destroy(&region->batcher);
    pthread_mutex_destroy(&region->epoch_lock);
    pthread_mutex_destroy(&region->seg_lock);

    free(region);
}

void* tm_start(shared_t shared) {
    region_t* region = (region_t*)shared;
    return region->first_segment->user_base;
}

size_t tm_size(shared_t shared) {
    region_t* region = (region_t*)shared;
    return region->first_seg_size;
}

size_t tm_align(shared_t shared) {
    region_t* region = (region_t*)shared;
    return region->align;
}

tx_t tm_begin(shared_t shared, bool is_ro) {
    region_t* region = (region_t*)shared;

    // Allocate transaction first
    tx_internal_t* tx = (tx_internal_t*)calloc(1, sizeof(tx_internal_t));
    if (!tx) {
        return invalid_tx;
    }

    // Enter batcher
    batcher_enter(&region->batcher);

    tx->region = region;
    tx->is_ro = is_ro;
    tx->tx_id = atomic_fetch_add(&region->next_tx_id, 1);
    tx->aborted = false;

    return (tx_t)tx;
}

bool tm_end(shared_t unused(shared), tx_t tx_handle) {
    tx_internal_t* tx = (tx_internal_t*)tx_handle;
    region_t* region = tx->region;

    bool success = !tx->aborted;

    if (success && !tx->is_ro) {
        // Commit: register writes for epoch end (batch operation)
        if (tx->write_count > 0) {
            add_epoch_writes_batch(region, tx->writes, tx->write_count);
        }

        // Commit: register frees for epoch end
        for (size_t i = 0; i < tx->free_count; i++) {
            add_epoch_free(region, tx->frees[i]);
        }
    }

    // Cleanup and leave batcher
    tx_cleanup(tx, success);
    tx_destroy(tx);

    return success;
}

bool tm_read(shared_t unused(shared), tx_t tx_handle, void const* source, size_t size, void* target) {
    tx_internal_t* tx = (tx_internal_t*)tx_handle;
    region_t* region = tx->region;

    if (tx->aborted) return false;

    // Find segment
    segment_t* seg = find_segment_unlocked(region, source);
    if (!seg) {
        tx->aborted = true;
        tx_cleanup(tx, false);
        return false;
    }

    // Calculate word range
    size_t offset = (const uint8_t*)source - (const uint8_t*)seg->user_base;
    size_t start_word = offset / region->align;
    size_t word_count = size / region->align;

    // Read word by word
    for (size_t i = 0; i < word_count; i++) {
        if (!read_word(tx, seg, start_word + i, (uint8_t*)target + i * region->align)) {
            tx->aborted = true;
            tx_cleanup(tx, false);
            return false;
        }
    }

    return true;
}

bool tm_write(shared_t unused(shared), tx_t tx_handle, void const* source, size_t size, void* target) {
    tx_internal_t* tx = (tx_internal_t*)tx_handle;
    region_t* region = tx->region;

    if (tx->aborted) return false;

    // Find segment
    segment_t* seg = find_segment_unlocked(region, target);
    if (!seg) {
        tx->aborted = true;
        tx_cleanup(tx, false);
        return false;
    }

    // Calculate word range
    size_t offset = (const uint8_t*)target - (const uint8_t*)seg->user_base;
    size_t start_word = offset / region->align;
    size_t word_count = size / region->align;

    // Write word by word
    for (size_t i = 0; i < word_count; i++) {
        if (!write_word(tx, seg, start_word + i, (const uint8_t*)source + i * region->align)) {
            tx->aborted = true;
            tx_cleanup(tx, false);
            return false;
        }
    }

    return true;
}

alloc_t tm_alloc(shared_t unused(shared), tx_t tx_handle, size_t size, void** target) {
    tx_internal_t* tx = (tx_internal_t*)tx_handle;
    region_t* region = tx->region;

    if (tx->aborted) return abort_alloc;

    // Create new segment
    segment_t* seg = segment_create(size, region->align);
    if (!seg) return nomem_alloc;

    // Add to segment registry
    pthread_mutex_lock(&region->seg_lock);
    seg->next = region->segments;
    region->segments = seg;
    pthread_mutex_unlock(&region->seg_lock);

    // Add to transaction's alloc list
    if (!tx_add_alloc(tx, seg)) {
        pthread_mutex_lock(&region->seg_lock);
        segment_t** pp = &region->segments;
        while (*pp) {
            if (*pp == seg) {
                *pp = seg->next;
                break;
            }
            pp = &(*pp)->next;
        }
        pthread_mutex_unlock(&region->seg_lock);
        segment_destroy(seg);
        return nomem_alloc;
    }

    *target = seg->user_base;
    return success_alloc;
}

bool tm_free(shared_t unused(shared), tx_t tx_handle, void* target) {
    tx_internal_t* tx = (tx_internal_t*)tx_handle;
    region_t* region = tx->region;

    if (tx->aborted) return false;

    // Find segment
    segment_t* seg = find_segment_unlocked(region, target);
    if (!seg || seg == region->first_segment) {
        tx->aborted = true;
        tx_cleanup(tx, false);
        return false;
    }

    // Add to transaction's free list
    if (!tx_add_free(tx, seg)) {
        tx->aborted = true;
        tx_cleanup(tx, false);
        return false;
    }

    return true;
}
