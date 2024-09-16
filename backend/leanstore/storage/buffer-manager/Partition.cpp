#include "Partition.h"
#include "leanstore/utils/Misc.hpp"

#include <sys/mman.h>
#include <cstring>

namespace leanstore {
namespace storage {

void *malloc_huge(size_t size) {
  void *p = mmap(NULL, size, PROT_READ | PROT_WRITE,
                 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  madvise(p, size, MADV_HUGEPAGE);
  memset(p, 0, size);
  return p;
}

HashTable::Entry::Entry(PageId key) : key(key) {}

HashTable::HashTable(uint64_t sizeInBits) {
  uint64_t size = (1ull << sizeInBits);
  mask = size - 1;
  entries = (Entry **)malloc_huge(size * sizeof(Entry *));
}

uint64_t HashTable::hashKey(PageId k) {
  // MurmurHash64A
  const uint64_t m = 0xc6a4a7935bd1e995ull;
  const int r = 47;
  uint64_t h = 0x8445d61a4e774912ull ^ (8 * m);
  k *= m;
  k ^= k >> r;
  k *= m;
  h ^= k;
  h *= m;
  h ^= h >> r;
  h *= m;
  h ^= h >> r;
  return h;
}

IOFrame &HashTable::insert(PageId key) {
  auto e = new Entry(key);
  uint64_t pos = hashKey(key) & mask;
  e->next = entries[pos];
  entries[pos] = e;
  return e->value;
}

HashTable::Handler HashTable::lookup(PageId key) {
  uint64_t pos = hashKey(key) & mask;
  Entry **entry_ptr = entries + pos;
  Entry *entry = *entry_ptr;  // e is only here for readability
  while (entry) {
    if (entry->key == key) {
      return {entry_ptr};
    }

    entry_ptr = &(entry->next);
    entry = entry->next;
  }
  return {nullptr};
}

void HashTable::doRemove(HashTable::Handler &handler) {
  Entry *to_delete = *handler.holder;
  *handler.holder = (*handler.holder)->next;
  delete to_delete;
}

void HashTable::remove(PageId key) {
  auto handler = lookup(key);
  assert(handler.isValid());
  doRemove(handler);
}

bool HashTable::has(PageId key) {
  uint64_t pos = hashKey(key) & mask;
  auto entry = entries[pos];
  while (entry) {
    if (entry->key == key) {
      return true;
    }
    entry = entry->next;
  }
  return false;
}

Partition::Partition(PageId first_page_id, uint64_t page_id_distance,
                     uint64_t free_bfs_limit)
    : io_table(utils::getBitsNeeded(free_bfs_limit)),
      dram_free_list(free_bfs_limit),
      page_id_distance(page_id_distance),
      next_page_id(first_page_id) {}

}  // namespace storage
}  // namespace leanstore
