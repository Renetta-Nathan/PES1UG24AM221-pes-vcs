// object.c — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).
//
// PROVIDED functions: compute_hash, object_path, object_exists, hash_to_hex, hex_to_hash
// TODO functions:     object_write, object_read

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>

#ifdef _WIN32
#include <io.h>
#include <direct.h>
#define mkdir(path, mode) _mkdir(path)
#define fsync(fd) _commit(fd)
#endif

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

// Get the filesystem path where an object should be stored.
// Format: .pes/objects/XX/YYYYYYYY...
// The first 2 hex chars form the shard directory; the rest is the filename.
void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── TODO: Implement these ──────────────────────────────────────────────────

// Write an object to the store.
//
// Object format on disk:
//   "<type> <size>\0<data>"
//   where <type> is "blob", "tree", or "commit"
//   and <size> is the decimal string of the data length
//
// Steps:
//   1. Build the full object: header ("blob 16\0") + data
//   2. Compute SHA-256 hash of the FULL object (header + data)
//   3. Check if object already exists (deduplication) — if so, just return success
//   4. Create shard directory (.pes/objects/XX/) if it doesn't exist
//   5. Write to a temporary file in the same shard directory
//   6. fsync() the temporary file to ensure data reaches disk
//   7. rename() the temp file to the final path (atomic on POSIX)
//   8. Open and fsync() the shard directory to persist the rename
//   9. Store the computed hash in *id_out

// HINTS - Useful syscalls and functions for this phase:
//   - sprintf / snprintf : formatting the header string
//   - compute_hash       : hashing the combined header + data
//   - object_exists      : checking for deduplication
//   - mkdir              : creating the shard directory (use mode 0755)
//   - open, write, close : creating and writing to the temp file
//                          (Use O_CREAT | O_WRONLY | O_TRUNC, mode 0644)
//   - fsync              : flushing the file descriptor to disk
//   - rename             : atomically moving the temp file to the final path
//

//
// Returns 0 on success, -1 on error.
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    const char *type_str = (type == OBJ_BLOB) ? "blob" : (type == OBJ_TREE) ? "tree" : "commit";
    
    // 1. Build the full object
    char header[64];
    int header_len = sprintf(header, "%s %zu", type_str, len) + 1; // +1 for \0
    
    size_t full_len = header_len + len;
    void *full_data = malloc(full_len);
    if (!full_data) return -1;
    
    memcpy(full_data, header, header_len);
    memcpy((char *)full_data + header_len, data, len);
    
    // 2. Compute SHA-256 hash
    compute_hash(full_data, full_len, id_out);
    
    // 3. Deduplication
    if (object_exists(id_out)) {
        free(full_data);
        return 0;
    }
    
    // 4. Create shard directory
    char path[512];
    object_path(id_out, path, sizeof(path));
    
    char shard_dir[512];
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id_out, hex);
    snprintf(shard_dir, sizeof(shard_dir), "%s/%.2s", OBJECTS_DIR, hex);
    mkdir(shard_dir, 0755);
    
    // 5. Write to temporary file
    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s/%s.tmp", shard_dir, hex + 2);
    
#ifndef O_BINARY
#define O_BINARY 0
#endif

    int fd = open(tmp_path, O_CREAT | O_WRONLY | O_TRUNC | O_BINARY, 0644);
    if (fd < 0) {
        free(full_data);
        return -1;
    }
    
    if (write(fd, full_data, full_len) != (ssize_t)full_len) {
        close(fd);
        unlink(tmp_path);
        free(full_data);
        return -1;
    }
    
    // 6. fsync data
    fsync(fd);
    close(fd);
    
    // 7. rename to final path
    if (rename(tmp_path, path) != 0) {
        unlink(tmp_path);
        free(full_data);
        return -1;
    }
    
    // 8. fsync directory (best practice for persistence on POSIX, not applicable on Windows)
#ifndef _WIN32
    int dfd = open(shard_dir, O_RDONLY);
    if (dfd >= 0) {
        fsync(dfd);
        close(dfd);
    }
#endif
    
    free(full_data);
    return 0;
}

// Read an object from the store.
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    char path[512];
    object_path(id, path, sizeof(path));
    
    FILE *f = fopen(path, "rb");
    if (!f) return -1;
    
    fseek(f, 0, SEEK_END);
    size_t full_len = ftell(f);
    fseek(f, 0, SEEK_SET);
    
    void *full_data = malloc(full_len);
    if (!full_data || fread(full_data, 1, full_len, f) != full_len) {
        if (full_data) free(full_data);
        fclose(f);
        return -1;
    }
    fclose(f);
    
    // Integrity check
    ObjectID actual_id;
    compute_hash(full_data, full_len, &actual_id);
    if (memcmp(id->hash, actual_id.hash, HASH_SIZE) != 0) {
        free(full_data);
        return -1;
    }
    
    // Parse header
    char *header = (char *)full_data;
    char *null_byte = memchr(header, '\0', full_len);
    if (!null_byte) {
        free(full_data);
        return -1;
    }
    
    size_t header_len = (null_byte - header) + 1;
    char type_str[16];
    size_t data_size;
    if (sscanf(header, "%15s %zu", type_str, &data_size) != 2) {
        free(full_data);
        return -1;
    }
    
    if (strcmp(type_str, "blob") == 0) *type_out = OBJ_BLOB;
    else if (strcmp(type_str, "tree") == 0) *type_out = OBJ_TREE;
    else if (strcmp(type_str, "commit") == 0) *type_out = OBJ_COMMIT;
    else {
        free(full_data);
        return -1;
    }
    
    *len_out = data_size;
    *data_out = malloc(data_size);
    if (!*data_out) {
        free(full_data);
        return -1;
    }
    
    memcpy(*data_out, (char *)full_data + header_len, data_size);
    free(full_data);
    
    return 0;
}
