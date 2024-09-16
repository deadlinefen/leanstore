#pragma once
#include "Units.hpp"
#include "Swip.h"
#include "leanstore/sync-primitives/Latch.hpp"

#include <atomic>
#include <cstring>

namespace leanstore {
namespace storage {

constexpr const uint64_t PAGE_SIZE = 4 * 1024;
struct BufferFrame {
  constexpr static uint64_t kPageAlignedSize = 512;

  enum class State : uint8_t { FREE = 0, HOT = 1, COOL = 2, LOADED = 3 };
  struct Header {
    WorkerId last_writer_worker_id =
        std::numeric_limits<uint8_t>::max();  // for RFA
    LogId last_written_plsn = 0;
    State state = State::FREE;  // INIT:
    std::atomic<bool> is_being_written_back = false;
    bool keep_in_memory = false;
    PageId page_id = 9999;  // INIT:
    HybridLatch latch = 0;  // INIT: // ATTENTION: NEVER DECREMENT

    BufferFrame *next_free_bf = nullptr;

    // Contention Split data structure
    struct ContentionTracker {
      uint32_t restarts_counter = 0;
      uint32_t access_counter = 0;
      int32_t last_modified_pos = -1;
      void reset() {
        restarts_counter = 0;
        access_counter = 0;
        last_modified_pos = -1;
      }
    };

    struct OptimisticParentPointer {
      BufferFrame *parent_buffer_frame = nullptr;
      PageId parent_page_id;
      LogId parent_plsn = 0;
      BufferFrame **swip_ptr = nullptr;
      int64_t pos_in_parent = -1;
      void update(BufferFrame *new_parent_bf, PageId new_parent_pid,
                  LogId new_parent_gsn, BufferFrame **new_swip_ptr,
                  int64_t new_pos_in_parent) {
        if (parent_buffer_frame != new_parent_bf ||
            parent_page_id != new_parent_pid || parent_plsn != new_parent_gsn ||
            swip_ptr != new_swip_ptr || pos_in_parent != new_pos_in_parent) {
          parent_buffer_frame = new_parent_bf;
          parent_page_id = new_parent_pid;
          parent_plsn = new_parent_gsn;
          swip_ptr = new_swip_ptr;
          pos_in_parent = new_pos_in_parent;
        }
      }
    };

    ContentionTracker contention_tracker;
    OptimisticParentPointer optimistic_parent_pointer;
    uint64_t crc = 0;
  };

  struct alignas(kPageAlignedSize) Page {
    LogId PLSN = 0;
    LogId GSN = 0;
    DataStructureId data_structure_id = 9999;  // INIT: datastructure id
    uint64_t magic_debugging_number;           // ATTENTION
    uint8_t
        data[PAGE_SIZE - sizeof(PLSN) - sizeof(GSN) -
             sizeof(data_structure_id) -
             sizeof(magic_debugging_number)];  // Datastruture BE CAREFUL HERE
                                               // !!!!!

    operator uint8_t *() { return reinterpret_cast<uint8_t *>(this); }
  };

  struct Header header;
  struct Page page;  // The persisted part

  bool operator==(const BufferFrame &other) { return this == &other; }

  inline bool isDirty() const { return page.PLSN != header.last_written_plsn; }
  inline bool isFree() const { return header.state == State::FREE; }

  // Pre: bf is exclusively locked
  void reset() {
    header.crc = 0;

    assert(!header.is_being_written_back);
    header.latch.assertExclusivelyLatched();
    header.last_writer_worker_id = std::numeric_limits<uint8_t>::max();
    header.last_written_plsn = 0;
    header.state = State::FREE;  // INIT:
    header.is_being_written_back.store(false, std::memory_order_release);
    header.page_id = 9999;
    header.next_free_bf = nullptr;
    header.contention_tracker.reset();
    header.keep_in_memory = false;
    // std::memset(reinterpret_cast<uint8_t*>(&page), 0, PAGE_SIZE);
  }

  BufferFrame() { header.latch->store(0ul); }
};

static constexpr uint64_t EFFECTIVE_PAGE_SIZE = sizeof(BufferFrame::Page::data);

static_assert(sizeof(BufferFrame::Page) == PAGE_SIZE, "");

static_assert((sizeof(BufferFrame) - sizeof(BufferFrame::Page)) == 512, "");

}  // namespace storage
}  // namespace leanstore
