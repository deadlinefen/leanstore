#pragma once

// #include "BufferFrame.h"
#include "Units.hpp"
#include <cstdint>

namespace leanstore {
namespace storage {
struct BufferFrame;  // Forward declaration

template <typename T>
class Swip {
  // 1xxxxxxxxxxxx evicted, 01xxxxxxxxxxx cooling, 00xxxxxxxxxxx hot
  constexpr static const uint64_t evicted_bit = 1ULL << 63;
  constexpr static const uint64_t evicted_mask = ~(1ULL << 63);
  constexpr static const uint64_t cool_bit = 1ULL << 62;
  constexpr static const uint64_t cool_mask = ~(1ULL << 62);
  constexpr static const uint64_t hot_mask = ~(3ULL << 62);
  static_assert(evicted_bit == 0x8000000000000000, "");
  static_assert(evicted_mask == 0x7FFFFFFFFFFFFFFF, "");
  static_assert(hot_mask == 0x3FFFFFFFFFFFFFFF, "");

 public:
  union {
    uint64_t page_id;
    BufferFrame *buffer_frame;
  };

  Swip() = default;
  Swip(BufferFrame *bf) : buffer_frame(bf) {}
  template <typename T2>
  Swip(Swip<T2> &other) : page_id(other.page_id) {}

  bool operator==(const Swip &other) const { return (raw() == other.raw()); }

  bool isHOT() { return (page_id & (evicted_bit | cool_bit)) == 0; }
  bool isCOOL() { return page_id & cool_bit; }
  bool isEVICTED() { return page_id & evicted_bit; }

  uint64_t asPageId() { return page_id & evicted_mask; }
  BufferFrame &asBufferFrame() { return *buffer_frame; }
  BufferFrame &asBufferFrameMasked() {
    return *reinterpret_cast<BufferFrame *>(page_id & hot_mask);
  }
  uint64_t raw() const { return page_id; }

  template <typename T2>
  void warm(T2 *buffer_frame) {
    this->buffer_frame = buffer_frame;
  }
  void warm() {
    assert(isCOOL());
    this->page_id = page_id & ~cool_bit;
  }

  void cool() { this->page_id = page_id | cool_bit; }

  void evict(PageId page_id) { this->page_id = page_id | evicted_bit; }

  template <typename T2>
  Swip<T2> &cast() {
    return *reinterpret_cast<Swip<T2> *>(this);
  }
};

}  // namespace storage
}  // namespace leanstore
