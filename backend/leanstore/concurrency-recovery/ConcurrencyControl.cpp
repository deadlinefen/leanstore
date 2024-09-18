#include "Worker.hpp"
#include "leanstore/storage/buffer-manager/DTRegistry.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/utils/Misc.hpp"
// -------------------------------------------------------------------------------------
#include <set>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
atomic<u64> Worker::ConcurrencyControl::global_clock = WORKERS_INCREMENT;
// -------------------------------------------------------------------------------------
// Also for interval garbage collection
void Worker::ConcurrencyControl::refreshGlobalState()
{
   if (!FLAGS_todo) {
      // Why bother
      return;
   }
   utils::Timer timer(CRCounters::myCounters().cc_ms_refresh_global_state);
   if (utils::RandomGenerator::getRandU64(0, my().workers_count) == 0 && global_mutex.try_lock()) {
      TXID local_newest_olap = std::numeric_limits<u64>::min();
      TXID local_oldest_oltp = std::numeric_limits<u64>::max();
      TXID local_oldest_tx = std::numeric_limits<u64>::max();

      for (WorkerId w_i = 0; w_i < my().workers_count; w_i++) {
         u64 its_in_flight_tx_id = global_workers_current_snapshot[w_i].load();
         // -------------------------------------------------------------------------------------
         while ((its_in_flight_tx_id & LATCH_BIT) && ((its_in_flight_tx_id & CLEAN_BITS_MASK) < activeTX().startTS())) {
            its_in_flight_tx_id = global_workers_current_snapshot[w_i].load();
         }
         // -------------------------------------------------------------------------------------
         const bool is_rc = its_in_flight_tx_id & RC_BIT;
         const bool is_olap = its_in_flight_tx_id & OLAP_BIT;
         its_in_flight_tx_id &= CLEAN_BITS_MASK;
         if (!is_rc) {
            local_oldest_tx = std::min<TXID>(its_in_flight_tx_id, local_oldest_tx);
            if (is_olap) {
               local_newest_olap = std::max<TXID>(its_in_flight_tx_id, local_newest_olap);
            } else {
               local_oldest_oltp = std::min<TXID>(its_in_flight_tx_id, local_oldest_oltp);
            }
         }
      }
      // -------------------------------------------------------------------------------------
      global_oldest_all_start_ts.store(local_oldest_tx, std::memory_order_release);
      global_oldest_oltp_start_ts.store(local_oldest_oltp, std::memory_order_release);
      global_newest_olap_start_ts.store(local_newest_olap, std::memory_order_release);
      // -------------------------------------------------------------------------------------
      TXID global_all_lwm_buffer = std::numeric_limits<TXID>::max();
      TXID global_oltp_lwm_buffer = std::numeric_limits<TXID>::max();
      bool skipped_a_worker = false;
      for (WorkerId w_i = 0; w_i < my().workers_count; w_i++) {
         if (other(w_i).local_latest_lwm_for_tx == other(w_i).local_latest_write_tx) {
            skipped_a_worker = true;
            continue;
         } else {
            other(w_i).local_latest_lwm_for_tx.store(other(w_i).local_latest_write_tx, std::memory_order_release);
         }
         // -------------------------------------------------------------------------------------
         TXID its_all_lwm_buffer = other(w_i).commit_tree.LCB(global_oldest_all_start_ts),
              its_oltp_lwm_buffer = other(w_i).commit_tree.LCB(global_oldest_oltp_start_ts);
         // -------------------------------------------------------------------------------------
         if (FLAGS_olap_mode && global_oldest_all_start_ts != global_oldest_oltp_start_ts) {
            // ensure(its_all_lwm_buffer <= its_oltp_lwm_buffer);
            global_oltp_lwm_buffer = std::min<TXID>(its_oltp_lwm_buffer, global_oltp_lwm_buffer);
         } else {
            its_oltp_lwm_buffer = its_all_lwm_buffer;
         }
         // -------------------------------------------------------------------------------------
         global_all_lwm_buffer = std::min<TXID>(its_all_lwm_buffer, global_all_lwm_buffer);
         // -------------------------------------------------------------------------------------
         other(w_i).local_lwm_latch.store(other(w_i).local_lwm_latch.load() + 1, std::memory_order_release);  // Latch
         other(w_i).all_lwm_receiver.store(its_all_lwm_buffer, std::memory_order_release);
         other(w_i).oltp_lwm_receiver.store(its_oltp_lwm_buffer, std::memory_order_release);
         other(w_i).local_lwm_latch.store(other(w_i).local_lwm_latch.load() + 1, std::memory_order_release);  // Release
      }
      if (!skipped_a_worker) {
         global_all_lwm.store(global_all_lwm_buffer, std::memory_order_release);
         global_oltp_lwm.store(global_oltp_lwm_buffer, std::memory_order_release);
      }
      // -------------------------------------------------------------------------------------
      global_mutex.unlock();
   }
}
// -------------------------------------------------------------------------------------
void Worker::ConcurrencyControl::switchToSnapshotIsolationMode()
{
   {
      std::unique_lock guard(global_mutex);
      global_workers_current_snapshot[my().worker_id].store(global_clock.load(), std::memory_order_release);
   }
   refreshGlobalState();
}
// -------------------------------------------------------------------------------------
void Worker::ConcurrencyControl::switchToReadCommittedMode()
{
   {
      // Latch-free work only when all counters increase monotone, we can not simply go back
      std::unique_lock guard(global_mutex);
      const u64 last_commit_mark_flagged = global_workers_current_snapshot[my().worker_id].load() | RC_BIT;
      global_workers_current_snapshot[my().worker_id].store(last_commit_mark_flagged, std::memory_order_release);
   }
   refreshGlobalState();
}
// -------------------------------------------------------------------------------------
void Worker::ConcurrencyControl::garbageCollection()
{
   if (!FLAGS_todo) {
      return;
   }
   // -------------------------------------------------------------------------------------
   // TODO: smooth purge, we should not let the system hang on this, as a quick fix, it should be enough if we purge in small batches
   utils::Timer timer(CRCounters::myCounters().cc_ms_gc);
synclwm : {
   u64 lwm_version = local_lwm_latch.load();
   while ((lwm_version = local_lwm_latch.load()) & 1)
      ;
   local_all_lwm = all_lwm_receiver.load();
   local_oltp_lwm = oltp_lwm_receiver.load();
   if (lwm_version != local_lwm_latch.load()) {
      goto synclwm;
   }
   ensure(!FLAGS_olap_mode || local_all_lwm <= local_oltp_lwm);
}
   // ATTENTION: atm, with out extra sync, the two lwm can not
   if (local_all_lwm > cleaned_untill_oltp_lwm) {
      utils::Timer timer(CRCounters::myCounters().cc_ms_gc_history_tree);
      // PURGE!
      history_tree.purgeVersions(
          my().worker_id, 0, local_all_lwm - 1,
          [&](const TXID tx_id, const DataStructureId dt_id, const u8* version_payload, [[maybe_unused]] u64 version_payload_length, const bool called_before) {
             leanstore::storage::DTRegistry::global_dt_registry.todo(dt_id, version_payload, my().worker_id, tx_id, called_before);
             COUNTERS_BLOCK()
             {
                WorkerCounters::myCounters().cc_todo_olap_executed[dt_id]++;
             }
          },
          0);
      cleaned_untill_oltp_lwm = std::max(local_all_lwm, cleaned_untill_oltp_lwm);
   }
   if (FLAGS_olap_mode && local_all_lwm != local_oltp_lwm) {
      if (FLAGS_graveyard && local_oltp_lwm > 0 && local_oltp_lwm > cleaned_untill_oltp_lwm) {
         utils::Timer timer(CRCounters::myCounters().cc_ms_gc_graveyard);
         // MOVE deletes to the graveyard
         const u64 from_tx_id = cleaned_untill_oltp_lwm > 0 ? cleaned_untill_oltp_lwm : 0;
         history_tree.visitRemoveVersions(my().worker_id, from_tx_id, local_oltp_lwm - 1,
                                          [&](const TXID tx_id, const DataStructureId dt_id, const u8* version_payload,
                                              [[maybe_unused]] u64 version_payload_length, const bool called_before) {
                                             cleaned_untill_oltp_lwm = std::max(cleaned_untill_oltp_lwm, tx_id + 1);
                                             leanstore::storage::DTRegistry::global_dt_registry.todo(dt_id, version_payload, my().worker_id, tx_id,
                                                                                                     called_before);
                                             COUNTERS_BLOCK()
                                             {
                                                WorkerCounters::myCounters().cc_todo_oltp_executed[dt_id]++;
                                             }
                                          });
      }
   }
}
Worker::ConcurrencyControl::VISIBILITY Worker::ConcurrencyControl::isVisibleForIt(WorkerId whom_worker_id, TXID commit_ts)
{
   return local_workers_start_ts[whom_worker_id] > commit_ts ? VISIBILITY::VISIBLE_ALREADY : VISIBILITY::VISIBLE_NEXT_ROUND;
}
// -------------------------------------------------------------------------------------
// UNDETERMINED is not possible atm because we spin on start_ts
Worker::ConcurrencyControl::VISIBILITY Worker::ConcurrencyControl::isVisibleForIt(WorkerId whom_worker_id, WorkerId what_worker_id, TXID tx_ts)
{
   const bool is_commit_ts = tx_ts & MSB;
   const TXID commit_ts = is_commit_ts ? (tx_ts & MSB_MASK) : getCommitTimestamp(what_worker_id, tx_ts);
   return isVisibleForIt(whom_worker_id, commit_ts);
}
// -------------------------------------------------------------------------------------
TXID Worker::ConcurrencyControl::getCommitTimestamp(WorkerId worker_id, TXID tx_ts)
{
   if (tx_ts & MSB) {
      return tx_ts & MSB_MASK;
   }
   assert((tx_ts & MSB) || isVisibleForMe(worker_id, tx_ts));
   // -------------------------------------------------------------------------------------
   const TXID& start_ts = tx_ts;
   TXID lcb = other(worker_id).commit_tree.LCB(start_ts);
   TXID commit_ts = lcb ? lcb : std::numeric_limits<TXID>::max();  // TODO: align with GC
   ensure(commit_ts > start_ts);
   return commit_ts;
}
// -------------------------------------------------------------------------------------
// It is also used to check whether the tuple is write-locked, hence we need the to_write intention flag
bool Worker::ConcurrencyControl::isVisibleForMe(WorkerId other_worker_id, u64 tx_ts, bool to_write)
{
   const bool is_commit_ts = tx_ts & MSB;
   const TXID committed_ts = (tx_ts & MSB) ? (tx_ts & MSB_MASK) : 0;
   const TXID start_ts = tx_ts & MSB_MASK;
   if (!to_write && activeTX().isReadUncommitted()) {
      return true;
   }
   if (my().worker_id == other_worker_id) {
      return true;
   }
   // -------------------------------------------------------------------------------------
   if (activeTX().isReadCommitted() || activeTX().isReadUncommitted()) {
      if (is_commit_ts) {
         return true;
      }
      if (local_snapshot_cache[other_worker_id] >= start_ts) {  // use cache
         return true;
      } else {
         utils::Timer timer(CRCounters::myCounters().cc_ms_snapshotting);
         TXID current_ts = cr::Worker::my().cc.global_clock.load() + 1;
         TXID largest_visibile_ts_start = other(other_worker_id).commit_tree.LCB(current_ts);
         local_snapshot_cache[other_worker_id] = largest_visibile_ts_start;
         local_snapshot_cache_ts[other_worker_id] = current_ts;
         explainWhen(largest_visibile_ts_start < start_ts);
         return largest_visibile_ts_start >= start_ts;
      }
   } else if (activeTX().atLeastSI()) {
      if (is_commit_ts) {
         return my().active_tx.startTS() > committed_ts;
      }
      if (start_ts < local_global_all_lwm_cache) {
         return true;
      }
      // -------------------------------------------------------------------------------------
      if (local_snapshot_cache_ts[other_worker_id] == activeTX().startTS()) {  // Use the cache
         return local_snapshot_cache[other_worker_id] >= start_ts;
      } else if (local_snapshot_cache[other_worker_id] >= start_ts) {
         return true;
      }
      utils::Timer timer(CRCounters::myCounters().cc_ms_snapshotting);
      TXID largest_visible_tx_id = other(other_worker_id).commit_tree.LCB(my().active_tx.startTS());
      if (largest_visible_tx_id) {
         local_snapshot_cache[other_worker_id] = largest_visible_tx_id;
         local_snapshot_cache_ts[other_worker_id] = my().active_tx.startTS();
         return largest_visible_tx_id >= start_ts;
      }
      return false;
   } else {
      UNREACHABLE();
   }
}
// -------------------------------------------------------------------------------------
bool Worker::ConcurrencyControl::isVisibleForAll(WorkerId, TXID ts)
{
   if (ts & MSB) {
      // Commit Timestamp
      return (ts & MSB_MASK) < global_oldest_all_start_ts.load();
   } else {
      // Start Timestamp
      return ts < global_all_lwm.load();
   }
}
// -------------------------------------------------------------------------------------
TXID Worker::ConcurrencyControl::CommitTree::commit(TXID start_ts)
{
   utils::Timer timer(CRCounters::myCounters().cc_ms_committing);
   mutex.lock();
   assert(cursor < capacity);
   const TXID commit_ts = global_clock.fetch_add(1);
   array[cursor++] = {commit_ts, start_ts};
   mutex.unlock();
   return commit_ts;
}
// -------------------------------------------------------------------------------------
std::optional<std::pair<TXID, TXID>> Worker::ConcurrencyControl::CommitTree::LCBUnsafe(TXID start_ts)
{
   const auto begin = array;
   const auto end = array + cursor;
   auto it = std::lower_bound(begin, end, start_ts, [&](const auto& pair, TXID ts) { return pair.first < ts; });
   if (it == begin) {
      // raise(SIGTRAP);
      return {};
   } else {
      it--;
      assert(it->second < start_ts);
      return *it;
   }
}
// -------------------------------------------------------------------------------------
TXID Worker::ConcurrencyControl::CommitTree::LCB(TXID start_ts)
{
   TXID lcb = 0;
   mutex.lock_shared();
   auto v = LCBUnsafe(start_ts);
   if (v) {
      lcb = v->second;
   }
   mutex.unlock_shared();
   return lcb;
}
// -------------------------------------------------------------------------------------
void Worker::ConcurrencyControl::CommitTree::cleanIfNecessary()
{
   if (cursor < capacity) {
      return;
   }
   utils::Timer timer(CRCounters::myCounters().cc_ms_gc_cm);
   std::set<std::pair<TXID, TXID>> set;  // TODO: unordered_set
   const WorkerId my_worker_id = cr::Worker::my().worker_id;
   for (WorkerId w_i = 0; w_i < cr::Worker::my().workers_count; w_i++) {
      if (w_i == my_worker_id) {
         continue;
      }
      u64 its_start_ts = global_workers_current_snapshot[w_i].load();
      // -------------------------------------------------------------------------------------
      while (its_start_ts & LATCH_BIT) {
         its_start_ts = global_workers_current_snapshot[w_i].load();
      }
      its_start_ts &= Worker::CLEAN_BITS_MASK;
      set.insert(array[cursor - 1]);  // for  the new TX
      if (its_start_ts == 0) {        // to avoid race conditions when switching from RC to SI
         set.insert(array[0]);
      } else {
         auto v = LCBUnsafe(its_start_ts);
         if (v) {
            set.insert(*v);
         }
      }
   }
   // -------------------------------------------------------------------------------------
   mutex.lock();
   cursor = 0;
   for (auto& p : set) {
      array[cursor++] = p;
   }
   mutex.unlock();
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
