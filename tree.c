// tree.c — Tree object serialization and construction
//
// PROVIDED functions: get_file_mode, tree_parse, tree_serialize
// TODO functions:     tree_from_index
//
// Binary tree format (per entry, concatenated with no separators):
//   "<mode-as-ascii-octal> <name>\0<32-byte-binary-hash>"
//
// Example single entry (conceptual):
//   "100644 hello.txt\0" followed by 32 raw bytes of SHA-256

#include "tree.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include "index.h"

// Forward declarations (implemented in object.c)
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);

// ─── Mode Constants ─────────────────────────────────────────────────────────

#define MODE_FILE      0100644
#define MODE_EXEC      0100755
#define MODE_DIR       0040000

// ─── PROVIDED ───────────────────────────────────────────────────────────────

// Determine the object mode for a filesystem path.
uint32_t get_file_mode(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) return 0;

    if (S_ISDIR(st.st_mode))  return MODE_DIR;
    if (st.st_mode & S_IXUSR) return MODE_EXEC;
    return MODE_FILE;
}

// Parse binary tree data into a Tree struct safely.
// Returns 0 on success, -1 on parse error.
int tree_parse(const void *data, size_t len, Tree *tree_out) {
    tree_out->count = 0;
    const uint8_t *ptr = (const uint8_t *)data;
    const uint8_t *end = ptr + len;

    while (ptr < end && tree_out->count < MAX_TREE_ENTRIES) {
        TreeEntry *entry = &tree_out->entries[tree_out->count];

        // 1. Safely find the space character for the mode
        const uint8_t *space = memchr(ptr, ' ', end - ptr);
        if (!space) return -1; // Malformed data

        // Parse mode into an isolated buffer
        char mode_str[16] = {0};
        size_t mode_len = space - ptr;
        if (mode_len >= sizeof(mode_str)) return -1;
        memcpy(mode_str, ptr, mode_len);
        entry->mode = strtol(mode_str, NULL, 8);

        ptr = space + 1; // Skip space

        // 2. Safely find the null terminator for the name
        const uint8_t *null_byte = memchr(ptr, '\0', end - ptr);
        if (!null_byte) return -1; // Malformed data

        size_t name_len = null_byte - ptr;
        if (name_len >= sizeof(entry->name)) return -1;
        memcpy(entry->name, ptr, name_len);
        entry->name[name_len] = '\0'; // Ensure null-terminated

        ptr = null_byte + 1; // Skip null byte

        // 3. Read the 32-byte binary hash
        if (ptr + HASH_SIZE > end) return -1; 
        memcpy(entry->hash.hash, ptr, HASH_SIZE);
        ptr += HASH_SIZE;

        tree_out->count++;
    }
    return 0;
}

// Helper for qsort to ensure consistent tree hashing
static int compare_tree_entries(const void *a, const void *b) {
    return strcmp(((const TreeEntry *)a)->name, ((const TreeEntry *)b)->name);
}

// Serialize a Tree struct into binary format for storage.
// Caller must free(*data_out).
// Returns 0 on success, -1 on error.
int tree_serialize(const Tree *tree, void **data_out, size_t *len_out) {
    // Estimate max size: (6 bytes mode + 1 byte space + 256 bytes name + 1 byte null + 32 bytes hash) per entry
    size_t max_size = tree->count * 296; 
    uint8_t *buffer = malloc(max_size);
    if (!buffer) return -1;

    // Create a mutable copy to sort entries (Git requirement)
    Tree sorted_tree = *tree;
    qsort(sorted_tree.entries, sorted_tree.count, sizeof(TreeEntry), compare_tree_entries);

    size_t offset = 0;
    for (int i = 0; i < sorted_tree.count; i++) {
        const TreeEntry *entry = &sorted_tree.entries[i];
        
        // Write mode and name (%o writes octal correctly for Git standards)
        int written = sprintf((char *)buffer + offset, "%o %s", entry->mode, entry->name);
        offset += written + 1; // +1 to step over the null terminator written by sprintf
        
        // Write binary hash
        memcpy(buffer + offset, entry->hash.hash, HASH_SIZE);
        offset += HASH_SIZE;
    }

    *data_out = buffer;
    *len_out = offset;
    return 0;
}

// ─── TODO: Implement these ──────────────────────────────────────────────────

// Recursive helper to build tree objects from index entries.
static int write_tree_recursive(IndexEntry *entries, int count, int depth, ObjectID *id_out) {
    Tree *tree = malloc(sizeof(Tree));
    if (!tree) return -1;
    tree->count = 0;

    for (int i = 0; i < count; ) {
        const char *path = entries[i].path;
        
        // Skip depth '/' separators to find the name of the entry at this level
        const char *current_part = path;
        for (int d = 0; d < depth; d++) {
            const char *slash = strchr(current_part, '/');
            if (slash) current_part = slash + 1;
        }

        const char *slash = strchr(current_part, '/');
        if (slash == NULL) {
            // It's a file at this level
            if (tree->count >= MAX_TREE_ENTRIES) { free(tree); return -1; }
            TreeEntry *te = &tree->entries[tree->count++];
            te->mode = entries[i].mode;
            te->hash = entries[i].hash;
            strncpy(te->name, current_part, sizeof(te->name) - 1);
            te->name[sizeof(te->name) - 1] = '\0';
            i++;
        } else {
            // It's a directory
            char dirname[256];
            size_t dirlen = slash - current_part;
            if (dirlen >= sizeof(dirname)) dirlen = sizeof(dirname) - 1;
            strncpy(dirname, current_part, dirlen);
            dirname[dirlen] = '\0';

            // Find all entries that share this same directory at the current level
            int start = i;
            while (i < count) {
                const char *p = entries[i].path;
                const char *cp = p;
                for (int d = 0; d < depth; d++) {
                    const char *s = strchr(cp, '/');
                    if (s) cp = s + 1;
                }
                if (strncmp(cp, dirname, dirlen) == 0 && cp[dirlen] == '/') {
                    i++;
                } else {
                    break;
                }
            }

            if (tree->count >= MAX_TREE_ENTRIES) { free(tree); return -1; }
            TreeEntry *te = &tree->entries[tree->count++];
            te->mode = 0040000; // MODE_DIR
            strncpy(te->name, dirname, sizeof(te->name) - 1);
            te->name[sizeof(te->name) - 1] = '\0';
            
            if (write_tree_recursive(&entries[start], i - start, depth + 1, &te->hash) != 0) {
                free(tree);
                return -1;
            }
        }
    }

    void *data;
    size_t len;
    if (tree_serialize(tree, &data, &len) != 0) { free(tree); return -1; }
    int rc = object_write(OBJ_TREE, data, len, id_out);
    free(data);
    free(tree);
    return rc;
}

// Build a tree hierarchy from the current index and write all tree
// objects to the object store.
int tree_from_index(ObjectID *id_out) {
    Index *index = malloc(sizeof(Index));
    if (!index) return -1;
    if (index_load(index) != 0) { free(index); return -1; }
    if (index->count == 0) {
        // Empty tree
        Tree tree;
        tree.count = 0;
        void *data;
        size_t len;
        tree_serialize(&tree, &data, &len);
        int rc = object_write(OBJ_TREE, data, len, id_out);
        free(data);
        free(index);
        return rc;
    }
    int rc = write_tree_recursive(index->entries, index->count, 0, id_out);
    free(index);
    return rc;
}
