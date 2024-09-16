#include "DTRegistry.hpp"

#include "leanstore/profiling/counters/WorkerCounters.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
DTRegistry DTRegistry::global_dt_registry;
// -------------------------------------------------------------------------------------
void DTRegistry::iterateChildrenSwips(DataStructureId dtid, BufferFrame& bf, std::function<bool(Swip<BufferFrame>&)> callback)
{
   auto dt_meta = dt_instances_ht[dtid];
   dt_types_ht[std::get<0>(dt_meta)].iterate_children(std::get<1>(dt_meta), bf, callback);
}
// -------------------------------------------------------------------------------------
ParentSwipHandler DTRegistry::findParent(DataStructureId dtid, BufferFrame& bf)
{
   auto dt_meta = dt_instances_ht[dtid];
   auto name = std::get<2>(dt_meta);
   return dt_types_ht[std::get<0>(dt_meta)].find_parent(std::get<1>(dt_meta), bf);
}
// -------------------------------------------------------------------------------------
SpaceCheckResult DTRegistry::checkSpaceUtilization(DataStructureId dtid, BufferFrame& bf)
{
   auto dt_meta = dt_instances_ht[dtid];
   return dt_types_ht[std::get<0>(dt_meta)].check_space_utilization(std::get<1>(dt_meta), bf);
}
// -------------------------------------------------------------------------------------
void DTRegistry::checkpoint(DataStructureId dtid, BufferFrame& bf, u8* dest)
{
   auto dt_meta = dt_instances_ht[dtid];
   return dt_types_ht[std::get<0>(dt_meta)].checkpoint(std::get<1>(dt_meta), bf, dest);
}
// -------------------------------------------------------------------------------------
// Datastructures management
// -------------------------------------------------------------------------------------
void DTRegistry::registerDatastructureType(DTType type, DTRegistry::DTMeta dt_meta)
{
   std::unique_lock guard(mutex);
   dt_types_ht[type] = dt_meta;
}
// -------------------------------------------------------------------------------------
void DTRegistry::registerDatastructureInstance(DTType type, void* root_object, string name, DataStructureId dt_id)
{
   std::unique_lock guard(mutex);
   dt_instances_ht.insert({dt_id, {type, root_object, name}});
   if (dt_id >= instances_counter) {
      instances_counter = dt_id + 1;
   }
}
// -------------------------------------------------------------------------------------
DataStructureId DTRegistry::registerDatastructureInstance(DTType type, void* root_object, string name)
{
   std::unique_lock guard(mutex);
   DataStructureId new_instance_id = instances_counter++;
   dt_instances_ht.insert({new_instance_id, {type, root_object, name}});
   return new_instance_id;
}
// -------------------------------------------------------------------------------------
void DTRegistry::undo(DataStructureId dt_id, const u8* wal_entry, u64 tts)
{
   auto dt_meta = dt_instances_ht[dt_id];
   return dt_types_ht[std::get<0>(dt_meta)].undo(std::get<1>(dt_meta), wal_entry, tts);
}

// -------------------------------------------------------------------------------------
void DTRegistry::todo(DataStructureId dt_id, const u8* entry, const u64 version_worker_id, u64 version_tx_id, const bool called_before)
{
   auto dt_meta = dt_instances_ht[dt_id];
   return dt_types_ht[std::get<0>(dt_meta)].todo(std::get<1>(dt_meta), entry, version_worker_id, version_tx_id, called_before);
}
// -------------------------------------------------------------------------------------
void DTRegistry::unlock(DataStructureId dt_id, const u8* entry)
{
   auto dt_meta = dt_instances_ht[dt_id];
   return dt_types_ht[std::get<0>(dt_meta)].unlock(std::get<1>(dt_meta), entry);
}
// -------------------------------------------------------------------------------------
std::unordered_map<std::string, std::string> DTRegistry::serialize(DataStructureId dt_id)
{
   auto dt_meta = dt_instances_ht[dt_id];
   return dt_types_ht[std::get<0>(dt_meta)].serialize(std::get<1>(dt_meta));
}
// -------------------------------------------------------------------------------------
void DTRegistry::deserialize(DataStructureId dt_id, std::unordered_map<std::string, std::string> map)
{
   auto dt_meta = dt_instances_ht[dt_id];
   return dt_types_ht[std::get<0>(dt_meta)].deserialize(std::get<1>(dt_meta), map);
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
