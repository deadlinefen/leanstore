#pragma once
#include "BufferFrame.h"
#include "FreeList.h"
#include "Units.hpp"
#include "leanstore/Config.hpp"

#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <vector>

namespace leanstore {
namespace storage {

struct IOFrame {
  enum class STATE : uint8_t {
    READING = 0,
    READY = 1,
    TO_DELETE = 2,
    UNDEFINED = 3  // for debugging
  };
  std::mutex mutex;
  STATE state = STATE::UNDEFINED;
  BufferFrame *buffer_frame = nullptr;

  // Everything in CIOFrame is protected by partition lock
  // except the following counter which is decremented outside to determine
  // whether it is time to remove it
  std::atomic<int64_t> readers_counter = 0;
};

class HashTable {
 public:
  struct Entry {
    Entry(PageId key);

    PageId key;
    Entry *next;
    IOFrame value;
  };

  struct Handler {
    bool isValid() const { return holder != nullptr; }
    IOFrame &frame() const {
      assert(this->isValid());
      return *reinterpret_cast<IOFrame *>(&((*holder)->value));
    }

    Entry **holder;
  };

  HashTable(uint64_t size_in_bits);

  IOFrame &insert(PageId key);
  Handler lookup(PageId key);
  void remove(PageId key);
  bool has(uint64_t key);  // for debugging

 private:
  uint64_t hashKey(uint64_t k);
  void doRemove(Handler &handler);

  uint64_t mask;
  Entry **entries;
};

class Partition {
 public:
  Partition(uint64_t first_page_id, uint64_t page_id_distance,
            uint64_t free_bfs_limit);

  inline PageId allocatePageId() {
    std::unique_lock<std::shared_mutex> guard(page_ids_mutex);
    if (freed_page_ids.size()) {
      const uint64_t page_id = freed_page_ids.back();
      freed_page_ids.pop_back();
      return page_id;
    } else {
      const uint64_t page_id = next_page_id;
      next_page_id += page_id_distance;
      ensure((page_id * PAGE_SIZE / 1024 / 1024 / 1024) <= FLAGS_ssd_gib);
      return page_id;
    }
  }
  void freePage(PageId pid) {
    std::unique_lock<std::shared_mutex> guard(page_ids_mutex);
    freed_page_ids.push_back(pid);
  }

  inline PageId getNextPageId() { return next_page_id; }
  inline void setNextPageId(PageId id) { next_page_id = id; }

  uint64_t AllocatedPageCount() { return next_page_id / page_id_distance; }
  uint64_t FreedPageCount() {
    std::shared_lock<std::shared_mutex> guard(page_ids_mutex);
    return freed_page_ids.size();
  }

  std::mutex io_table_mutex;
  HashTable io_table;
  FreeList dram_free_list;

 private:
  // SSD Pages
  const uint64_t page_id_distance;
  std::shared_mutex page_ids_mutex;  // protect free pids vector
  std::vector<PageId> freed_page_ids;
  PageId next_page_id;
};

}  // namespace storage
}  // namespace leanstore
