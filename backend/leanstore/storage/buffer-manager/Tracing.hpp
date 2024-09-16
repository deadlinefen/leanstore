#pragma once
#include "BufferFrame.h"
#include "FreeList.h"
#include "Units.hpp"
#include "leanstore/Config.hpp"


#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace leanstore
{
namespace storage
{

class Tracing
{
  public:
   static std::mutex mutex;
   static std::unordered_map<PageId, std::tuple<DataStructureId, u64>> ht;
   static void printStatus(PageId pid)
   {
      mutex.lock();
      if (ht.contains(pid)) {
         cout << pid << " was written out: " << std::get<1>(ht[pid]) << " times form DT: " << std::get<0>(ht[pid]) << endl;
      } else {
         cout << pid << " was never written out" << endl;
      }
      mutex.unlock();
   }
};

}  // namespace storage
}  // namespace leanstore
