#pragma once
#include "BufferFrame.h"
#include "DTRegistry.hpp"
#include "FreeList.h"
#include "Partition.h"
#include "Swip.h"
#include "Units.hpp"

#include <libaio.h>
#include <sys/mman.h>

#include <cstring>
#include <unordered_map>

namespace leanstore {
class LeanStore;  // Forward declaration
namespace profiling {
class BMTable;  // Forward declaration
}
namespace storage {

class FreedBufferframesBatch {
 public:
  FreedBufferframesBatch() : head(nullptr), tail(nullptr), counter(0) {}

  void Reset() {
    head = nullptr;
    tail = nullptr;
    counter = 0;
  }

  void Push(Partition &partition) {
    partition.dram_free_list.batchPush(head, tail, counter);
    Reset();
  }

  uint64_t Size() { return counter; }

  void Add(BufferFrame &buffer_frame) {
    buffer_frame.header.next_free_bf = head;
    if (head == nullptr) {
      tail = &buffer_frame;
    }
    head = &buffer_frame;
    ++counter;
  }

 private:
  BufferFrame *head;
  BufferFrame *tail;
  uint64_t counter;
};

// TODO: revisit the comments after switching to clock replacement strategy
// Notes on Synchronization in Buffer Manager
// Terminology: PPT: Page Provider Thread, WT: Worker Thread. P: Parent, C:
// Child, M: Cooling stage mutex Latching order for all PPT operations
// (unswizzle, evict): M -> P -> C Latching order for all WT operations:
// swizzle: [unlock P ->] M -> P ->C, coolPage: P -> C -> M coolPage conflict
// with this order which could lead to a deadlock which we can mitigate by
// jumping instead of blocking in BMPlainGuard [WIP]

class BufferManager {
 public:
  BufferManager(int32_t ssd_fd);
  ~BufferManager();

  BufferFrame &allocatePage();
  inline BufferFrame &tryFastResolveSwip(HybridLatchGuard &swip_guard,
                                         Swip<BufferFrame> &swip_value) {
    if (swip_value.isHOT()) {
      BufferFrame &bf = swip_value.asBufferFrame();
      swip_guard.recheck();
      return bf;
    } else {
      return ResolveSwip(swip_guard, swip_value);
    }
  }
  BufferFrame &ResolveSwip(HybridLatchGuard &swip_guard, Swip<BufferFrame> &swip_value);
  void evictLastPage();
  void reclaimPage(BufferFrame &bf);

  /*
   * Life cycle of a fix:
   * 1- Check if the pid is swizzled, if yes then store the BufferFrame address
   * temporarily 2- if not, then posix_check if it exists in cooling stage
   * queue, yes? remove it from the queue and return the buffer frame 3- in
   * anycase, posix_check if the threshold is exceeded, yes ? unswizzle a random
   * BufferFrame (or its children if needed) then add it to the cooling stage.
   */

  void readPageSync(PageId pid, uint8_t *destination);
  void readPageAsync(PageId pid, uint8_t *destination,
                     std::function<void()> callback);
  void fDataSync();

  void startBackgroundThreads();
  void stopBackgroundThreads();
  void writeAllBufferFrames();
  std::unordered_map<std::string, std::string> serialize();
  void deserialize(std::unordered_map<std::string, std::string> map);

  uint64_t getPoolSize() { return dram_pool_size; }
  DTRegistry &getDTRegistry() { return DTRegistry::global_dt_registry; }
  uint64_t consumedPages();
  BufferFrame &getContainingBufferFrame(
      const uint8_t
          *);  // get the buffer frame containing the given ptr address

 private:
  friend class leanstore::LeanStore;
  friend class leanstore::profiling::BMTable;

  BufferFrame *buffer_frames;

  const int ssd_fd;

  // Free  Pages
  // we reserve these extra pages to prevent segfaults
  const uint8_t safety_pages = 10;
  // total number of dram buffer frames
  uint64_t dram_pool_size;
  // used to track how many pages did we really allocate
  atomic<uint64_t> ssd_freed_pages_counter = 0;

  // For cooling and inflight io
  uint64_t partitions_count;
  uint64_t partitions_mask;
  std::vector<std::unique_ptr<Partition>> partitions;
  std::atomic<uint64_t> clock_cursor = 0;

  // Threads managements
  void pageProviderThread(uint64_t p_begin,
                          uint64_t p_end);  // [p_begin, p_end)
  atomic<uint64_t> bg_threads_counter = 0;
  atomic<bool> bg_threads_keep_running = true;

  // Misc
  Partition &randomPartition();
  BufferFrame &randomBufferFrame();
  Partition &getPartition(PageId);
  uint64_t getPartitionID(PageId);

  // Temporary hack: let workers evict the last page they used
  static thread_local BufferFrame *last_read_bf;
};  // namespace storage

class BMC {
 public:
  static BufferManager *global_bf;
};
}  // namespace storage
}  // namespace leanstore
