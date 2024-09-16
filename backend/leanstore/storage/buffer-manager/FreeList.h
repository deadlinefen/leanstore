#pragma once
#include "BufferFrame.h"

#include <cstdint>
#include <mutex>

namespace leanstore {
namespace storage {

class FreeList {
 public:
  explicit FreeList(uint64_t capacity) : capacity(capacity) {}

  BufferFrame &tryPop();
  void batchPush(BufferFrame *head, BufferFrame *tail, uint64_t counter);
  void push(BufferFrame &bf);
  inline uint64_t size() { return counter.load(); }
  inline bool notReachedLimit() { return counter < capacity; }

 private:
  std::mutex mutex;
  BufferFrame *head = nullptr;
  std::atomic<uint64_t> counter = 0;
  const uint64_t capacity;
};

}  // namespace storage
}  // namespace leanstore
