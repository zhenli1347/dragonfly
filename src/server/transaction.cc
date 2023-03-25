// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/transaction.h"

#include <absl/strings/match.h>

#include "base/logging.h"
#include "server/blocking_controller.h"
#include "server/command_registry.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/journal/journal.h"
#include "server/server_state.h"

namespace dfly {

using namespace std;
using namespace util;
using absl::StrCat;

thread_local Transaction::TLTmpSpace Transaction::tmp_space;

namespace {

atomic_uint64_t op_seq{1};

[[maybe_unused]] constexpr size_t kTransSize = sizeof(Transaction);

}  // namespace

IntentLock::Mode Transaction::Mode() const {
  return (cid_->opt_mask() & CO::READONLY) ? IntentLock::SHARED : IntentLock::EXCLUSIVE;
}

/**
 * @brief Construct a new Transaction:: Transaction object
 *
 * @param cid
 * @param ess
 * @param cs
 */
Transaction::Transaction(const CommandId* cid, uint32_t thread_index)
    : cid_{cid}, coordinator_index_(thread_index) {
  string_view cmd_name(cid_->name());
  if (cmd_name == "EXEC" || cmd_name == "EVAL" || cmd_name == "EVALSHA") {
    multi_.reset(new MultiData);
    multi_->shard_journal_write.resize(shard_set->size(), false);

    multi_->mode = NOT_DETERMINED;
  }
}

Transaction::~Transaction() {
  DVLOG(3) << "Transaction " << StrCat(Name(), "@", txid_, "/", unique_shard_cnt_, ")")
           << " destroyed";
}

void Transaction::InitBase(DbIndex dbid, CmdArgList args) {
  global_ = false;
  db_index_ = dbid;
  cmd_with_full_args_ = args;
  local_result_ = OpStatus::OK;
}

void Transaction::InitGlobal() {
  DCHECK(!multi_ || (multi_->mode == GLOBAL || multi_->mode == NON_ATOMIC));

  global_ = true;
  unique_shard_cnt_ = shard_set->size();
  shard_data_.resize(unique_shard_cnt_);
  for (auto& sd : shard_data_)
    sd.local_mask = ACTIVE;
}

void Transaction::BuildShardIndex(KeyIndex key_index, bool rev_mapping,
                                  std::vector<PerShardCache>* out) {
  auto args = cmd_with_full_args_;

  auto& shard_index = *out;

  auto add = [this, rev_mapping, &shard_index](uint32_t sid, uint32_t i) {
    string_view val = ArgS(cmd_with_full_args_, i);
    shard_index[sid].args.push_back(val);
    if (rev_mapping)
      shard_index[sid].original_index.push_back(i - 1);
  };

  if (key_index.bonus) {
    DCHECK(key_index.step == 1);
    uint32_t sid = Shard(ArgS(args, key_index.bonus), shard_data_.size());
    add(sid, key_index.bonus);
  }

  for (unsigned i = key_index.start; i < key_index.end; ++i) {
    uint32_t sid = Shard(ArgS(args, i), shard_data_.size());
    add(sid, i);

    DCHECK_LE(key_index.step, 2u);
    if (key_index.step == 2) {  // Handle value associated with preceding key.
      add(sid, ++i);
    }
  }
}

void Transaction::InitShardData(absl::Span<const PerShardCache> shard_index, size_t num_args,
                                bool rev_mapping) {
  args_.reserve(num_args);
  if (rev_mapping)
    reverse_index_.reserve(args_.size());

  // Store the concatenated per-shard arguments from the shard index inside args_
  // and make each shard data point to its own sub-span inside args_.
  for (size_t i = 0; i < shard_data_.size(); ++i) {
    auto& sd = shard_data_[i];
    auto& si = shard_index[i];

    CHECK_LT(si.args.size(), 1u << 15);

    sd.arg_count = si.args.size();
    sd.arg_start = args_.size();

    if (multi_) {
      // Multi transactions can re-intitialize on different shards, so clear ACTIVE flag.
      sd.local_mask &= ~ACTIVE;

      // If we increase locks, clear KEYLOCK_ACQUIRED to track new locks.
      if (multi_->IsIncrLocks())
        sd.local_mask &= ~KEYLOCK_ACQUIRED;
    }

    if (sd.arg_count == 0 && !si.requested_active)
      continue;

    sd.local_mask |= ACTIVE;

    unique_shard_cnt_++;
    unique_shard_id_ = i;

    for (size_t j = 0; j < si.args.size(); ++j) {
      args_.push_back(si.args[j]);
      if (rev_mapping)
        reverse_index_.push_back(si.original_index[j]);
    }
  }

  CHECK(args_.size() == num_args);
}

void Transaction::InitMultiData(KeyIndex key_index) {
  DCHECK(multi_);
  auto args = cmd_with_full_args_;

  if (multi_->mode == NON_ATOMIC)
    return;

  // TODO: determine correct locking mode for transactions, scripts and regular commands.
  IntentLock::Mode mode = Mode();
  multi_->keys.clear();

  auto& tmp_uniques = tmp_space.uniq_keys;
  tmp_uniques.clear();

  auto lock_key = [this, mode, &tmp_uniques](string_view key) {
    if (auto [_, inserted] = tmp_uniques.insert(key); !inserted)
      return;

    if (multi_->IsIncrLocks()) {
      multi_->keys.emplace_back(key);
    } else {
      multi_->lock_counts[key][mode]++;
    }
  };

  // With EVAL, we call this function for EVAL itself as well as for each command
  // for eval. currently, we lock everything only during the eval call.
  if (multi_->IsIncrLocks() || !multi_->locks_recorded) {
    for (size_t i = key_index.start; i < key_index.end; i += key_index.step)
      lock_key(ArgS(args, i));
    if (key_index.bonus > 0)
      lock_key(ArgS(args, key_index.bonus));
  }

  multi_->locks_recorded = true;
  DCHECK(IsAtomicMulti());
  DCHECK(multi_->mode == GLOBAL || !multi_->keys.empty() || !multi_->lock_counts.empty());
}

void Transaction::StoreKeysInArgs(KeyIndex key_index, bool rev_mapping) {
  DCHECK_EQ(key_index.bonus, 0U);

  auto args = cmd_with_full_args_;

  // even for a single key we may have multiple arguments per key (MSET).
  for (unsigned j = key_index.start; j < key_index.start + key_index.step; ++j) {
    args_.push_back(ArgS(args, j));
  }

  if (rev_mapping) {
    reverse_index_.resize(args_.size());
    for (unsigned j = 0; j < reverse_index_.size(); ++j) {
      reverse_index_[j] = j + key_index.start - 1;
    }
  }
}

/**
 *
 * There are 4 options that we consider here:
 * a. T spans a single shard and its not multi.
 *    unique_shard_id_ is predefined before the schedule() is called.
 *    In that case only a single thread will be scheduled and it will use shard_data[0] just because
 *    shard_data.size() = 1. Coordinator thread can access any data because there is a
 *    schedule barrier between InitByArgs and RunInShard/IsArmedInShard functions.
 * b. T spans multiple shards and its not multi
 *    In that case multiple threads will be scheduled. Similarly they have a schedule barrier,
 *    and IsArmedInShard can read any variable from shard_data[x].
 * c. Trans spans a single shard and it's multi. shard_data has size of ess_.size.
 *    IsArmedInShard will check shard_data[x].
 * d. Trans spans multiple shards and it's multi. Similarly shard_data[x] will be checked.
 *    unique_shard_cnt_ and unique_shard_id_ are not accessed until shard_data[x] is armed, hence
 *    we have a barrier between coordinator and engine-threads. Therefore there should not be
 *    data races.
 *
 **/

void Transaction::InitByKeys(KeyIndex key_index) {
  auto args = cmd_with_full_args_;

  if (key_index.start == args.size()) {  // eval with 0 keys.
    CHECK(absl::StartsWith(cid_->name(), "EVAL"));
    return;
  }

  DCHECK_LT(key_index.start, args.size());

  bool needs_reverse_mapping = cid_->opt_mask() & CO::REVERSE_MAPPING;
  bool single_key = key_index.HasSingleKey();

  if (single_key && !IsAtomicMulti()) {
    DCHECK_GT(key_index.step, 0u);

    // We don't have to split the arguments by shards, so we can copy them directly.
    StoreKeysInArgs(key_index, needs_reverse_mapping);

    shard_data_.resize(IsMulti() ? shard_set->size() : 1);
    shard_data_.front().local_mask |= ACTIVE;

    unique_shard_cnt_ = 1;
    unique_shard_id_ = Shard(args_.front(), shard_set->size());

    return;
  }

  shard_data_.resize(shard_set->size());  // shard_data isn't sparse, so we must allocate for all :(
  CHECK(key_index.step == 1 || key_index.step == 2);
  DCHECK(key_index.step == 1 || (args.size() % 2) == 1);

  // Safe, because flow below is not preemptive.
  auto& shard_index = tmp_space.GetShardIndex(shard_data_.size());

  // Distribute all the arguments by shards.
  BuildShardIndex(key_index, needs_reverse_mapping, &shard_index);

  // Initialize shard data based on distributed arguments.
  InitShardData(shard_index, key_index.num_args(), needs_reverse_mapping);

  if (multi_)
    InitMultiData(key_index);

  DVLOG(1) << "InitByArgs " << DebugId() << " " << args_.front();

  // Compress shard data, if we occupy only one shard.
  if (unique_shard_cnt_ == 1) {
    PerShardData* sd;
    if (IsMulti()) {
      sd = &shard_data_[unique_shard_id_];
    } else {
      shard_data_.resize(1);
      sd = &shard_data_.front();
    }
    sd->local_mask |= ACTIVE;
    sd->arg_count = -1;
    sd->arg_start = -1;
  }

  // Validation. Check reverse mapping was built correctly.
  if (needs_reverse_mapping) {
    for (size_t i = 0; i < args_.size(); ++i) {
      DCHECK_EQ(args_[i], ArgS(args, 1 + reverse_index_[i]));  // 1 for the commandname.
    }
  }

  // Validation.
  for (const auto& sd : shard_data_) {
    // sd.local_mask may be non-zero for multi transactions with instant locking.
    // Specifically EVALs may maintain state between calls.
    DCHECK(!sd.is_armed.load(std::memory_order_relaxed));
    if (!multi_) {
      DCHECK_EQ(TxQueue::kEnd, sd.pq_pos);
    }
  }
}

OpStatus Transaction::InitByArgs(DbIndex index, CmdArgList args) {
  InitBase(index, args);

  if ((cid_->opt_mask() & CO::GLOBAL_TRANS) > 0) {
    InitGlobal();
    return OpStatus::OK;
  }

  CHECK_GT(args.size(), 1U);  // first entry is the command name.
  DCHECK_EQ(unique_shard_cnt_, 0u);
  DCHECK(args_.empty());

  OpResult<KeyIndex> key_index = DetermineKeys(cid_, args);
  if (!key_index)
    return key_index.status();

  InitByKeys(*key_index);
  return OpStatus::OK;
}

void Transaction::StartMultiGlobal(DbIndex dbid) {
  CHECK(multi_);
  CHECK(shard_data_.empty());  // Make sure default InitByArgs didn't run.

  multi_->mode = GLOBAL;
  InitBase(dbid, {});
  InitGlobal();
  multi_->locks_recorded = true;

  ScheduleInternal();
}

void Transaction::StartMultiLockedAhead(DbIndex dbid, CmdArgList keys) {
  DCHECK(multi_);
  DCHECK(shard_data_.empty());  // Make sure default InitByArgs didn't run.

  multi_->mode = LOCK_AHEAD;
  InitBase(dbid, keys);
  InitByKeys(KeyIndex::Range(0, keys.size()));

  ScheduleInternal();
}

void Transaction::StartMultiLockedIncr(DbIndex dbid, const vector<bool>& shards) {
  DCHECK(multi_);
  DCHECK(shard_data_.empty());  // Make sure default InitByArgs didn't run.
  DCHECK(std::any_of(shards.begin(), shards.end(), [](bool s) { return s; }));

  multi_->mode = LOCK_INCREMENTAL;
  InitBase(dbid, {});

  auto& shard_index = tmp_space.GetShardIndex(shard_set->size());
  for (size_t i = 0; i < shards.size(); i++)
    shard_index[i].requested_active = shards[i];

  shard_data_.resize(shard_index.size());
  InitShardData(shard_index, 0, false);

  ScheduleInternal();
}

void Transaction::StartMultiNonAtomic() {
  DCHECK(multi_);
  multi_->mode = NON_ATOMIC;
}

void Transaction::MultiSwitchCmd(const CommandId* cid) {
  DCHECK(multi_);
  DCHECK(!cb_);

  unique_shard_id_ = 0;
  unique_shard_cnt_ = 0;
  args_.clear();
  cid_ = cid;
  cb_ = nullptr;

  if (multi_->mode == NON_ATOMIC) {
    for (auto& sd : shard_data_) {
      sd.arg_count = sd.arg_start = sd.local_mask = 0;
      sd.pq_pos = TxQueue::kEnd;
      DCHECK_EQ(sd.is_armed.load(memory_order_relaxed), false);
    }
    txid_ = 0;
    coordinator_state_ = 0;
  }
}

string Transaction::DebugId() const {
  DCHECK_GT(use_count_.load(memory_order_relaxed), 0u);

  return StrCat(Name(), "@", txid_, "/", unique_shard_cnt_, " (", trans_id(this), ")");
}

// Runs in the dbslice thread. Returns true if transaction needs to be kept in the queue.
bool Transaction::RunInShard(EngineShard* shard) {
  DCHECK_GT(run_count_.load(memory_order_relaxed), 0u);
  CHECK(cb_) << DebugId();
  DCHECK_GT(txid_, 0u);

  // Unlike with regular transactions we do not acquire locks upon scheduling
  // because Scheduling is done before multi-exec batch is executed. Therefore we
  // lock keys right before the execution of each statement.

  unsigned idx = SidToId(shard->shard_id());
  auto& sd = shard_data_[idx];

  bool prev_armed = sd.is_armed.load(memory_order_relaxed);
  DCHECK(prev_armed);
  sd.is_armed.store(false, memory_order_relaxed);

  VLOG(2) << "RunInShard: " << DebugId() << " sid:" << shard->shard_id() << " " << sd.local_mask;

  bool was_suspended = sd.local_mask & SUSPENDED_Q;
  bool awaked_prerun = sd.local_mask & AWAKED_Q;
  bool incremental_lock = multi_ && multi_->IsIncrLocks();

  // For multi we unlock transaction (i.e. its keys) in UnlockMulti() call.
  // Therefore we differentiate between concluding, which says that this specific
  // runnable concludes current operation, and should_release which tells
  // whether we should unlock the keys. should_release is false for multi and
  // equal to concluding otherwise.
  bool is_concluding = (coordinator_state_ & COORD_EXEC_CONCLUDING);
  bool should_release = is_concluding && !IsAtomicMulti();
  IntentLock::Mode mode = Mode();

  // We make sure that we lock exactly once for each (multi-hop) transaction inside
  // transactions that lock incrementally.
  if (!IsGlobal() && incremental_lock && ((sd.local_mask & KEYLOCK_ACQUIRED) == 0)) {
    DCHECK(!awaked_prerun);  // we should not have a blocking transaction inside multi block.

    sd.local_mask |= KEYLOCK_ACQUIRED;
    shard->db_slice().Acquire(mode, GetLockArgs(idx));
  }

  DCHECK(IsGlobal() || (sd.local_mask & KEYLOCK_ACQUIRED) || (multi_ && multi_->mode == GLOBAL));

  /*************************************************************************/
  // Actually running the callback.
  // If you change the logic here, also please change the logic
  try {
    OpStatus status = OpStatus::OK;

    // if a transaction is suspended, we still run it because of brpoplpush/blmove case
    // that needs to run lpush on its suspended shard.
    status = cb_(this, shard);

    if (unique_shard_cnt_ == 1) {
      cb_ = nullptr;  // We can do it because only a single thread runs the callback.
      local_result_ = status;
    } else {
      if (status == OpStatus::OUT_OF_MEMORY) {
        local_result_ = status;
      } else {
        CHECK_EQ(OpStatus::OK, status);
      }
    }
  } catch (std::bad_alloc&) {
    // TODO: to log at most once per sec.
    LOG_FIRST_N(ERROR, 16) << " out of memory";
    local_result_ = OpStatus::OUT_OF_MEMORY;
  } catch (std::exception& e) {
    LOG(FATAL) << "Unexpected exception " << e.what();
  }

  /*************************************************************************/

  if (is_concluding)  // Check last hop
    LogAutoJournalOnShard(shard);

  // at least the coordinator thread owns the reference.
  DCHECK_GE(GetUseCount(), 1u);

  // we remove tx from tx-queue upon first invocation.
  // if it needs to run again it runs via a dedicated continuation_trans_ state in EngineShard.
  if (sd.pq_pos != TxQueue::kEnd) {
    shard->txq()->Remove(sd.pq_pos);
    sd.pq_pos = TxQueue::kEnd;
  }

  // If it's a final hop we should release the locks.
  if (should_release) {
    bool became_suspended = sd.local_mask & SUSPENDED_Q;
    KeyLockArgs largs;

    if (IsGlobal()) {
      DCHECK(!awaked_prerun && !became_suspended);  // Global transactions can not be blocking.
      shard->shard_lock()->Release(Mode());
    } else {  // not global.
      largs = GetLockArgs(idx);
      DCHECK(sd.local_mask & KEYLOCK_ACQUIRED);

      // If a transaction has been suspended, we keep the lock so that future transaction
      // touching those keys will be ordered via TxQueue. It's necessary because we preserve
      // the atomicity of awaked transactions by halting the TxQueue.
      if (was_suspended || !became_suspended) {
        shard->db_slice().Release(mode, largs);
        sd.local_mask &= ~KEYLOCK_ACQUIRED;
      }
      sd.local_mask &= ~OUT_OF_ORDER;
    }

    // It has 2 responsibilities.
    // 1: to go over potential wakened keys, verify them and activate watch queues.
    // 2: if this transaction was notified and finished running - to remove it from the head
    //    of the queue and notify the next one.
    // RunStep is also called for global transactions because of commands like MOVE.
    if (shard->blocking_controller()) {
      if (awaked_prerun || was_suspended) {
        shard->blocking_controller()->FinalizeWatched(largs, this);
      }
      shard->blocking_controller()->NotifyPending();
    }
  }

  CHECK_GE(DecreaseRunCnt(), 1u);
  // From this point on we can not access 'this'.

  return !should_release;  // keep
}

void Transaction::ScheduleInternal() {
  DCHECK(!shard_data_.empty());
  DCHECK_EQ(0u, txid_);
  DCHECK_EQ(0, coordinator_state_ & (COORD_SCHED | COORD_OOO));

  bool span_all = IsGlobal();

  uint32_t num_shards;
  std::function<bool(uint32_t)> is_active;

  // TODO: For multi-transactions we should be able to deduce mode() at run-time based
  // on the context. For regular multi-transactions we can actually inspect all commands.
  // For eval-like transactions - we can decided based on the command flavor (EVAL/EVALRO) or
  // auto-tune based on the static analysis (by identifying commands with hardcoded command names).
  IntentLock::Mode mode = Mode();

  if (span_all) {
    is_active = [](uint32_t) { return true; };
    num_shards = shard_set->size();

    // Lock shards
    auto cb = [mode](EngineShard* shard) { shard->shard_lock()->Acquire(mode); };
    shard_set->RunBriefInParallel(std::move(cb));
  } else {
    num_shards = unique_shard_cnt_;
    DCHECK_GT(num_shards, 0u);

    is_active = [&](uint32_t i) {
      return num_shards == 1 ? (i == unique_shard_id_) : shard_data_[i].local_mask & ACTIVE;
    };
  }

  // Loop until successfully scheduled in all shards.
  while (true) {
    txid_ = op_seq.fetch_add(1, memory_order_relaxed);
    time_now_ms_ = GetCurrentTimeMs();

    atomic_uint32_t lock_granted_cnt{0};
    atomic_uint32_t success{0};

    auto cb = [&](EngineShard* shard) {
      auto [is_success, is_granted] = ScheduleInShard(shard);
      success.fetch_add(is_success, memory_order_relaxed);
      lock_granted_cnt.fetch_add(is_granted, memory_order_relaxed);
    };
    shard_set->RunBriefInParallel(std::move(cb), is_active);

    bool ooo_disabled = IsGlobal() || (IsAtomicMulti() && multi_->mode != LOCK_AHEAD);

    if (success.load(memory_order_acquire) == num_shards) {
      coordinator_state_ |= COORD_SCHED;
      // If we granted all locks, we can run out of order.
      if (!ooo_disabled && lock_granted_cnt.load(memory_order_relaxed) == num_shards) {
        // Currently we don't support OOO for incremental locking. Sp far they are global.
        coordinator_state_ |= COORD_OOO;
      }
      VLOG(2) << "Scheduled " << DebugId()
              << " OutOfOrder: " << bool(coordinator_state_ & COORD_OOO)
              << " num_shards: " << num_shards;

      break;
    }

    VLOG(2) << "Cancelling " << DebugId();

    atomic_bool should_poll_execution{false};
    auto cancel = [&](EngineShard* shard) {
      bool res = CancelShardCb(shard);
      if (res) {
        should_poll_execution.store(true, memory_order_relaxed);
      }
    };
    shard_set->RunBriefInParallel(std::move(cancel), is_active);

    // We must follow up with PollExecution because in rare cases with multi-trans
    // that follows this one, we may find the next transaction in the queue that is never
    // trigerred. Which leads to deadlock. I could solve this by adding PollExecution to
    // CancelShardCb above but then we would need to use the shard_set queue since PollExecution
    // is blocking. I wanted to avoid the additional latency for the general case of running
    // CancelShardCb because of the very rate case below. Therefore, I decided to just fetch the
    // indication that we need to follow up with PollExecution and then send it to shard_set queue.
    // We do not need to wait for this callback to finish - just make sure it will eventually run.
    // See https://github.com/dragonflydb/dragonfly/issues/150 for more info.
    if (should_poll_execution.load(memory_order_relaxed)) {
      for (uint32_t i = 0; i < shard_set->size(); ++i) {
        if (!is_active(i))
          continue;

        shard_set->Add(i, [] { EngineShard::tlocal()->PollExecution("cancel_cleanup", nullptr); });
      }
    }
  }

  if (IsOOO()) {
    for (auto& sd : shard_data_) {
      sd.local_mask |= OUT_OF_ORDER;
    }
  }
}

void Transaction::MultiData::AddLocks(IntentLock::Mode mode) {
  DCHECK(IsIncrLocks());
  for (auto& key : keys) {
    lock_counts[std::move(key)][mode]++;
  }
  keys.clear();
}

bool Transaction::MultiData::IsIncrLocks() const {
  return mode == LOCK_INCREMENTAL;
}

// Optimized "Schedule and execute" function for the most common use-case of a single hop
// transactions like set/mset/mget etc. Does not apply for more complicated cases like RENAME or
// BLPOP where a data must be read from multiple shards before performing another hop.
OpStatus Transaction::ScheduleSingleHop(RunnableType cb) {
  DCHECK(!cb_);
  cb_ = std::move(cb);

  DCHECK(IsAtomicMulti() || (coordinator_state_ & COORD_SCHED) == 0);  // Multi schedule in advance.
  coordinator_state_ |= (COORD_EXEC | COORD_EXEC_CONCLUDING);  // Single hop means we conclude.

  bool was_ooo = false;

  // If we run only on one shard and conclude, we can avoid scheduling at all
  // and directly dispatch the task to its destination shard.
  bool schedule_fast = (unique_shard_cnt_ == 1) && !IsGlobal() && !IsAtomicMulti();
  if (schedule_fast) {
    DCHECK_NE(unique_shard_id_, kInvalidSid);
    DCHECK(shard_data_.size() == 1 || multi_->mode == NON_ATOMIC);

    // IsArmedInShard() first checks run_count_ before shard_data, so use release ordering.
    shard_data_[SidToId(unique_shard_id_)].is_armed.store(true, memory_order_relaxed);
    run_count_.store(1, memory_order_release);

    time_now_ms_ = GetCurrentTimeMs();

    // NOTE: schedule_cb cannot update data on stack when run_fast is false.
    // This is because ScheduleSingleHop can finish before the callback returns.

    // This happens when ScheduleUniqueShard schedules into TxQueue (hence run_fast is false), and
    // then calls PollExecute that in turn runs the callback which calls DecreaseRunCnt. As a result
    // WaitForShardCallbacks below is unblocked before schedule_cb returns. However, if run_fast is
    // true, then we may mutate stack variables, but only before DecreaseRunCnt is called.
    auto schedule_cb = [this, &was_ooo] {
      bool run_fast = ScheduleUniqueShard(EngineShard::tlocal());
      if (run_fast) {
        was_ooo = true;
        // it's important to DecreaseRunCnt only for run_fast and after run_eager is assigned.
        // If DecreaseRunCnt were called before ScheduleUniqueShard finishes
        // then WaitForShardCallbacks below could exit before schedule_cb assigns return value
        // to run_eager and cause stack corruption.
        CHECK_GE(DecreaseRunCnt(), 1u);
      }
    };
    shard_set->Add(unique_shard_id_, std::move(schedule_cb));  // serves as a barrier.
  } else {
    // This transaction either spans multiple shards and/or is multi.

    if (!IsAtomicMulti())  // Multi schedule in advance.
      ScheduleInternal();

    if (multi_ && multi_->IsIncrLocks())
      multi_->AddLocks(Mode());

    ExecuteAsync();
  }

  DVLOG(2) << "ScheduleSingleHop before Wait " << DebugId() << " " << run_count_.load();
  WaitForShardCallbacks();
  DVLOG(2) << "ScheduleSingleHop after Wait " << DebugId();

  if (was_ooo)
    coordinator_state_ |= COORD_OOO;

  cb_ = nullptr;
  return local_result_;
}

// Runs in the coordinator fiber.
void Transaction::UnlockMulti() {
  VLOG(1) << "UnlockMulti " << DebugId();
  DCHECK(multi_);
  DCHECK_GE(GetUseCount(), 1u);  // Greater-equal because there may be callbacks in progress.

  if (multi_->mode == NON_ATOMIC)
    return;

  auto sharded_keys = make_shared<vector<KeyList>>(shard_set->size());
  while (!multi_->lock_counts.empty()) {
    auto entry = multi_->lock_counts.extract(multi_->lock_counts.begin());
    ShardId sid = Shard(entry.key(), sharded_keys->size());
    (*sharded_keys)[sid].emplace_back(std::move(entry.key()), entry.mapped());
  }

  unsigned shard_journals_cnt =
      ServerState::tlocal()->journal() ? CalcMultiNumOfShardJournals() : 0;

  uint32_t prev = run_count_.fetch_add(shard_data_.size(), memory_order_relaxed);
  DCHECK_EQ(prev, 0u);

  use_count_.fetch_add(shard_data_.size(), std::memory_order_relaxed);
  for (ShardId i = 0; i < shard_data_.size(); ++i) {
    shard_set->Add(i, [this, sharded_keys, shard_journals_cnt]() {
      this->UnlockMultiShardCb(*sharded_keys, EngineShard::tlocal(), shard_journals_cnt);
      intrusive_ptr_release(this);
    });
  }

  VLOG(1) << "UnlockMultiEnd " << DebugId();
}

uint32_t Transaction::CalcMultiNumOfShardJournals() const {
  uint32_t shard_journals_cnt = 0;
  for (bool was_shard_write : multi_->shard_journal_write) {
    if (was_shard_write) {
      ++shard_journals_cnt;
    }
  }
  return shard_journals_cnt;
}

void Transaction::Schedule() {
  if (multi_ && multi_->IsIncrLocks())
    multi_->AddLocks(Mode());

  if (!IsAtomicMulti())
    ScheduleInternal();
}

// Runs in coordinator thread.
void Transaction::Execute(RunnableType cb, bool conclude) {
  DCHECK(coordinator_state_ & COORD_SCHED);

  cb_ = std::move(cb);
  coordinator_state_ |= COORD_EXEC;

  if (conclude) {
    coordinator_state_ |= COORD_EXEC_CONCLUDING;
  } else {
    coordinator_state_ &= ~COORD_EXEC_CONCLUDING;
  }

  ExecuteAsync();

  DVLOG(1) << "Wait on Exec " << DebugId();
  WaitForShardCallbacks();
  DVLOG(1) << "Wait on Exec " << DebugId() << " completed";

  cb_ = nullptr;
}

// Runs in coordinator thread.
void Transaction::ExecuteAsync() {
  DVLOG(1) << "ExecuteAsync " << DebugId();

  DCHECK_GT(unique_shard_cnt_, 0u);
  DCHECK_GT(use_count_.load(memory_order_relaxed), 0u);
  DCHECK(!IsAtomicMulti() || multi_->locks_recorded);

  // We do not necessarily Execute this transaction in 'cb' below. It well may be that it will be
  // executed by the engine shard once it has been armed and coordinator thread will finish the
  // transaction before engine shard thread stops accessing it. Therefore, we increase reference
  // by number of callbacks accessesing 'this' to allow callbacks to execute shard->Execute(this);
  // safely.
  use_count_.fetch_add(unique_shard_cnt_, memory_order_relaxed);

  // We access sd.is_armed outside of shard-threads but we guard it with run_count_ release.
  IterateActiveShards(
      [](PerShardData& sd, auto i) { sd.is_armed.store(true, memory_order_relaxed); });

  uint32_t seq = seqlock_.load(memory_order_relaxed);

  // this fence prevents that a read or write operation before a release fence will be reordered
  // with a write operation after a release fence. Specifically no writes below will be reordered
  // upwards. Important, because it protects non-threadsafe local_mask from being accessed by
  // IsArmedInShard in other threads.
  run_count_.store(unique_shard_cnt_, memory_order_release);

  // We verify seq lock has the same generation number. See below for more info.
  auto cb = [seq, this] {
    EngineShard* shard = EngineShard::tlocal();

    bool is_armed = IsArmedInShard(shard->shard_id());
    // First we check that this shard should run a callback by checking IsArmedInShard.
    if (is_armed) {
      uint32_t seq_after = seqlock_.load(memory_order_relaxed);

      DVLOG(3) << "PollExecCb " << DebugId() << " sid(" << shard->shard_id() << ") "
               << run_count_.load(memory_order_relaxed);

      // We also make sure that for mult-operation transactions like Multi/Eval
      // this callback runs on a correct operation. We want to avoid a situation
      // where the first operation is executed and the second operation is armed and
      // now this callback from the previous operation finally runs and calls PollExecution.
      // It is usually ok, but for single shard operations we abuse index 0 in shard_data_
      // Therefore we may end up with a situation where this old callback runs on shard 7,
      // accessing shard_data_[0] that now represents shard 5 for the next operation.
      // seqlock provides protection for that so each cb will only run on the operation it has
      // been tasked with.
      // We also must first check is_armed and only then seqlock. The first check ensures that
      // the coordinator thread crossed
      // "run_count_.store(unique_shard_cnt_, memory_order_release);" barrier and our seqlock_
      // is valid.
      if (seq_after == seq) {
        // shard->PollExecution(this) does not necessarily execute this transaction.
        // Therefore, everything that should be handled during the callback execution
        // should go into RunInShard.
        shard->PollExecution("exec_cb", this);
      } else {
        VLOG(1) << "Skipping PollExecution " << DebugId() << " sid(" << shard->shard_id() << ")";
      }
    }

    DVLOG(3) << "ptr_release " << DebugId() << " " << seq;
    intrusive_ptr_release(this);  // against use_count_.fetch_add above.
  };

  // IsArmedInShard is the protector of non-thread safe data.
  IterateActiveShards([&cb](PerShardData& sd, auto i) { shard_set->Add(i, cb); });
}

void Transaction::RunQuickie(EngineShard* shard) {
  DCHECK(!IsAtomicMulti());
  DCHECK(shard_data_.size() == 1u || multi_->mode == NON_ATOMIC);
  DCHECK_NE(unique_shard_id_, kInvalidSid);
  DCHECK_EQ(0u, txid_);

  shard->IncQuickRun();

  auto& sd = shard_data_[SidToId(unique_shard_id_)];
  DCHECK_EQ(0, sd.local_mask & (KEYLOCK_ACQUIRED | OUT_OF_ORDER));

  DVLOG(1) << "RunQuickSingle " << DebugId() << " " << shard->shard_id() << " " << args_[0];
  CHECK(cb_) << DebugId() << " " << shard->shard_id() << " " << args_[0];

  // Calling the callback in somewhat safe way
  try {
    local_result_ = cb_(this, shard);
  } catch (std::bad_alloc&) {
    LOG_FIRST_N(ERROR, 16) << " out of memory";
    local_result_ = OpStatus::OUT_OF_MEMORY;
  } catch (std::exception& e) {
    LOG(FATAL) << "Unexpected exception " << e.what();
  }

  LogAutoJournalOnShard(shard);

  sd.is_armed.store(false, memory_order_relaxed);
  cb_ = nullptr;  // We can do it because only a single shard runs the callback.
}

// runs in coordinator thread.
// Marks the transaction as expired and removes it from the waiting queue.
void Transaction::UnwatchBlocking(bool should_expire, WaitKeysProvider wcb) {
  DVLOG(1) << "UnwatchBlocking " << DebugId() << " expire: " << should_expire;
  DCHECK(!IsGlobal());

  run_count_.store(unique_shard_cnt_, memory_order_release);

  auto expire_cb = [this, &wcb, should_expire] {
    EngineShard* es = EngineShard::tlocal();
    ArgSlice wkeys = wcb(this, es);

    UnwatchShardCb(wkeys, should_expire, es);
  };

  IterateActiveShards([&expire_cb](PerShardData& sd, auto i) { shard_set->Add(i, expire_cb); });

  // Wait for all callbacks to conclude.
  WaitForShardCallbacks();
  DVLOG(1) << "UnwatchBlocking finished " << DebugId();
}

const char* Transaction::Name() const {
  return cid_->name();
}

KeyLockArgs Transaction::GetLockArgs(ShardId sid) const {
  KeyLockArgs res;
  res.db_index = db_index_;
  res.key_step = cid_->key_arg_step();
  res.args = GetShardArgs(sid);

  return res;
}

// Runs within a engine shard thread.
// Optimized path that schedules and runs transactions out of order if possible.
// Returns true if was eagerly executed, false if it was scheduled into queue.
bool Transaction::ScheduleUniqueShard(EngineShard* shard) {
  DCHECK(!IsAtomicMulti());
  DCHECK_EQ(0u, txid_);
  DCHECK(shard_data_.size() == 1u || multi_->mode == NON_ATOMIC);
  DCHECK_NE(unique_shard_id_, kInvalidSid);

  auto mode = Mode();
  auto lock_args = GetLockArgs(shard->shard_id());

  auto& sd = shard_data_[SidToId(unique_shard_id_)];
  DCHECK_EQ(TxQueue::kEnd, sd.pq_pos);

  // Fast path - for uncontended keys, just run the callback.
  // That applies for single key operations like set, get, lpush etc.
  if (shard->db_slice().CheckLock(mode, lock_args) && shard->shard_lock()->Check(mode)) {
    RunQuickie(shard);
    return true;
  }

  // we can do it because only a single thread writes into txid_ and sd.
  txid_ = op_seq.fetch_add(1, memory_order_relaxed);
  sd.pq_pos = shard->txq()->Insert(this);

  DCHECK_EQ(0, sd.local_mask & KEYLOCK_ACQUIRED);
  shard->db_slice().Acquire(mode, lock_args);
  sd.local_mask |= KEYLOCK_ACQUIRED;

  DVLOG(1) << "Rescheduling into TxQueue " << DebugId();

  shard->PollExecution("schedule_unique", nullptr);

  return false;
}

// This function should not block since it's run via RunBriefInParallel.
pair<bool, bool> Transaction::ScheduleInShard(EngineShard* shard) {
  DCHECK(!shard_data_.empty());
  DCHECK(shard_data_[SidToId(shard->shard_id())].local_mask & ACTIVE);

  // schedule_success, lock_granted.
  pair<bool, bool> result{false, false};

  if (shard->committed_txid() >= txid_) {
    return result;
  }

  TxQueue* txq = shard->txq();
  KeyLockArgs lock_args;
  IntentLock::Mode mode = Mode();

  bool spans_all = IsGlobal();
  bool lock_granted = false;
  ShardId sid = SidToId(shard->shard_id());

  auto& sd = shard_data_[sid];

  if (!spans_all) {
    bool shard_unlocked = shard->shard_lock()->Check(mode);
    lock_args = GetLockArgs(shard->shard_id());

    // we need to acquire the lock unrelated to shard_unlocked since we register into Tx queue.
    // All transactions in the queue must acquire the intent lock.
    lock_granted = shard->db_slice().Acquire(mode, lock_args) && shard_unlocked;
    sd.local_mask |= KEYLOCK_ACQUIRED;
    DVLOG(3) << "Lock granted " << lock_granted << " for trans " << DebugId();
  }

  if (!txq->Empty()) {
    // If the new transaction requires reordering of the pending queue (i.e. it comes before tail)
    // and some other transaction already locked its keys we can not reorder 'trans' because
    // that other transaction could have deduced that it can run OOO and eagerly execute. Hence, we
    // fail this scheduling attempt for trans.
    // However, when we schedule span-all transactions we can still reorder them. The reason is
    // before we start scheduling them we lock the shards and disable OOO.
    // We may record when they disable OOO via barrier_ts so if the queue contains transactions
    // that were only scheduled afterwards we know they are not free so we can still
    // reorder the queue. Currently, this optimization is disabled: barrier_ts < pq->HeadScore().
    bool to_proceed = lock_granted || txq->TailScore() < txid_;
    if (!to_proceed) {
      if (sd.local_mask & KEYLOCK_ACQUIRED) {  // rollback the lock.
        shard->db_slice().Release(mode, lock_args);
        sd.local_mask &= ~KEYLOCK_ACQUIRED;
      }

      return result;  // false, false
    }
  }

  result.second = lock_granted;
  result.first = true;

  TxQueue::Iterator it = txq->Insert(this);
  DCHECK_EQ(TxQueue::kEnd, sd.pq_pos);
  sd.pq_pos = it;

  DVLOG(1) << "Insert into tx-queue, sid(" << sid << ") " << DebugId() << ", qlen " << txq->size();

  return result;
}

bool Transaction::CancelShardCb(EngineShard* shard) {
  ShardId idx = SidToId(shard->shard_id());
  auto& sd = shard_data_[idx];

  auto pos = sd.pq_pos;
  if (pos == TxQueue::kEnd)
    return false;

  sd.pq_pos = TxQueue::kEnd;

  TxQueue* txq = shard->txq();
  TxQueue::Iterator head = txq->Head();
  auto val = txq->At(pos);
  Transaction* trans = absl::get<Transaction*>(val);
  DCHECK(trans == this) << "Pos " << pos << ", txq size " << txq->size() << ", trans " << trans;
  txq->Remove(pos);

  if (sd.local_mask & KEYLOCK_ACQUIRED) {
    auto mode = Mode();
    auto lock_args = GetLockArgs(shard->shard_id());
    DCHECK(lock_args.args.size() > 0 || (multi_ && multi_->mode == LOCK_INCREMENTAL));
    shard->db_slice().Release(mode, lock_args);
    sd.local_mask &= ~KEYLOCK_ACQUIRED;
  }

  if (pos == head && !txq->Empty()) {
    return true;
  }

  return false;
}

// runs in engine-shard thread.
ArgSlice Transaction::GetShardArgs(ShardId sid) const {
  DCHECK(!args_.empty() || (multi_ && multi_->IsIncrLocks()));

  // We can read unique_shard_cnt_  only because ShardArgsInShard is called after IsArmedInShard
  // barrier.
  if (unique_shard_cnt_ == 1) {
    return args_;
  }

  const auto& sd = shard_data_[sid];
  return ArgSlice{args_.data() + sd.arg_start, sd.arg_count};
}

// from local index back to original arg index skipping the command.
// i.e. returns (first_key_pos -1) or bigger.
size_t Transaction::ReverseArgIndex(ShardId shard_id, size_t arg_index) const {
  if (unique_shard_cnt_ == 1)
    return reverse_index_[arg_index];

  const auto& sd = shard_data_[shard_id];
  return reverse_index_[sd.arg_start + arg_index];
}

bool Transaction::WaitOnWatch(const time_point& tp, WaitKeysProvider wkeys_provider) {
  DVLOG(2) << "WaitOnWatch " << DebugId();
  using namespace chrono;

  auto cb = [&](Transaction* t, EngineShard* shard) {
    auto keys = wkeys_provider(t, shard);
    return t->WatchInShard(keys, shard);
  };

  Execute(move(cb), true);

  coordinator_state_ |= COORD_BLOCKED;

  auto wake_cb = [this] {
    return (coordinator_state_ & COORD_CANCELLED) ||
           notify_txid_.load(memory_order_relaxed) != kuint64max;
  };

  cv_status status = cv_status::no_timeout;
  if (tp == time_point::max()) {
    DVLOG(1) << "WaitOnWatch foreva " << DebugId();
    blocking_ec_.await(move(wake_cb));
    DVLOG(1) << "WaitOnWatch AfterWait";
  } else {
    DVLOG(1) << "WaitOnWatch TimeWait for "
             << duration_cast<milliseconds>(tp - time_point::clock::now()).count() << " ms "
             << DebugId();

    status = blocking_ec_.await_until(move(wake_cb), tp);

    DVLOG(1) << "WaitOnWatch await_until " << int(status);
  }

  bool is_expired = (coordinator_state_ & COORD_CANCELLED) || status == cv_status::timeout;
  UnwatchBlocking(is_expired, wkeys_provider);
  coordinator_state_ &= ~COORD_BLOCKED;

  return !is_expired;
}

// Runs only in the shard thread.
OpStatus Transaction::WatchInShard(ArgSlice keys, EngineShard* shard) {
  ShardId idx = SidToId(shard->shard_id());

  auto& sd = shard_data_[idx];
  CHECK_EQ(0, sd.local_mask & SUSPENDED_Q);

  auto* bc = shard->EnsureBlockingController();
  bc->AddWatched(keys, this);

  sd.local_mask |= SUSPENDED_Q;
  DVLOG(2) << "AddWatched " << DebugId() << " local_mask:" << sd.local_mask
           << ", first_key:" << keys.front();

  return OpStatus::OK;
}

void Transaction::UnwatchShardCb(ArgSlice wkeys, bool should_expire, EngineShard* shard) {
  if (should_expire) {
    auto lock_args = GetLockArgs(shard->shard_id());
    shard->db_slice().Release(Mode(), lock_args);

    unsigned sd_idx = SidToId(shard->shard_id());
    auto& sd = shard_data_[sd_idx];
    sd.local_mask |= EXPIRED_Q;
    sd.local_mask &= ~KEYLOCK_ACQUIRED;
    shard->blocking_controller()->FinalizeWatched(wkeys, this);
    DCHECK(!shard->blocking_controller()->awakened_transactions().contains(this));

    shard->blocking_controller()->NotifyPending();
  }

  // Need to see why I decided to call this.
  // My guess - probably to trigger the run of stalled transactions in case
  // this shard concurrently awoke this transaction and stalled the processing
  // of TxQueue.
  shard->PollExecution("unwatchcb", nullptr);

  CHECK_GE(DecreaseRunCnt(), 1u);
}

void Transaction::UnlockMultiShardCb(const std::vector<KeyList>& sharded_keys, EngineShard* shard,
                                     uint32_t shard_journals_cnt) {
#if 0
  auto journal = shard->journal();
  if (journal != nullptr && multi_->shard_journal_write[shard->shard_id()] == true) {
    journal->RecordEntry(txid_, journal::Op::EXEC, db_index_, shard_journals_cnt, {}, true);
  }
#endif
  if (multi_->mode == GLOBAL) {
    shard->shard_lock()->Release(IntentLock::EXCLUSIVE);
  } else {
    ShardId sid = shard->shard_id();
    for (const auto& k_v : sharded_keys[sid]) {
      auto release = [&](IntentLock::Mode mode) {
        if (k_v.second[mode]) {
          shard->db_slice().Release(mode, db_index_, k_v.first, k_v.second[mode]);
        }
      };

      release(IntentLock::SHARED);
      release(IntentLock::EXCLUSIVE);
    }
  }

  auto& sd = shard_data_[SidToId(shard->shard_id())];

  // It does not have to be that all shards in multi transaction execute this tx.
  // Hence it could stay in the tx queue. We perform the necessary cleanup and remove it from
  // there. The transaction is not guaranteed to be at front.
  if (sd.pq_pos != TxQueue::kEnd) {
    DVLOG(1) << "unlockmulti: TxRemove " << DebugId();

    TxQueue* txq = shard->txq();
    DCHECK(!txq->Empty());
    DCHECK_EQ(absl::get<Transaction*>(txq->At(sd.pq_pos)), this);

    txq->Remove(sd.pq_pos);
    sd.pq_pos = TxQueue::kEnd;
  }

  shard->ShutdownMulti(this);

  // notify awakened transactions, not sure we need it here because it's done after
  // each operation
  if (shard->blocking_controller())
    shard->blocking_controller()->NotifyPending();
  shard->PollExecution("unlockmulti", nullptr);

  this->DecreaseRunCnt();
}

inline uint32_t Transaction::DecreaseRunCnt() {
  // to protect against cases where Transaction is destroyed before run_ec_.notify
  // finishes running. We can not put it inside the (res == 1) block because then it's too late.
  ::boost::intrusive_ptr guard(this);

  // We use release so that no stores will be reordered after.
  uint32_t res = run_count_.fetch_sub(1, memory_order_release);
  if (res == 1) {
    run_ec_.notify();
  }
  return res;
}

bool Transaction::IsGlobal() const {
  return global_;
}

// Runs only in the shard thread.
// Returns true if the transacton has changed its state from suspended to awakened,
// false, otherwise.
bool Transaction::NotifySuspended(TxId committed_txid, ShardId sid) {
  unsigned idx = SidToId(sid);
  auto& sd = shard_data_[idx];
  unsigned local_mask = sd.local_mask;

  if (local_mask & Transaction::EXPIRED_Q) {
    return false;
  }

  DVLOG(1) << "NotifySuspended " << DebugId() << ", local_mask:" << local_mask << " by commited_id "
           << committed_txid;

  // local_mask could be awaked (i.e. not suspended) if the transaction has been
  // awakened by another key or awakened by the same key multiple times.
  if (local_mask & SUSPENDED_Q) {
    DCHECK_EQ(0u, local_mask & AWAKED_Q);

    sd.local_mask &= ~SUSPENDED_Q;
    sd.local_mask |= AWAKED_Q;

    TxId notify_id = notify_txid_.load(memory_order_relaxed);

    while (committed_txid < notify_id) {
      if (notify_txid_.compare_exchange_weak(notify_id, committed_txid, memory_order_relaxed)) {
        // if we improved notify_txid_ - break.
        blocking_ec_.notify();  // release barrier.
        break;
      }
    }
    return true;
  }

  CHECK(sd.local_mask & AWAKED_Q);
  return false;
}

void Transaction::LogAutoJournalOnShard(EngineShard* shard) {
  // TODO: For now, we ignore non shard coordination.
  if (shard == nullptr)
    return;

  // Ignore non-write commands or ones with disabled autojournal.
  if ((cid_->opt_mask() & CO::WRITE) == 0 || ((cid_->opt_mask() & CO::NO_AUTOJOURNAL) > 0 &&
                                              !renabled_auto_journal_.load(memory_order_relaxed)))
    return;

  auto journal = shard->journal();
  if (journal == nullptr)
    return;

  // TODO: Handle complex commands like LMPOP correctly once they are implemented.
  journal::Entry::Payload entry_payload;
  if (unique_shard_cnt_ == 1 || args_.empty()) {
    CHECK(!cmd_with_full_args_.empty());
    entry_payload = cmd_with_full_args_;
  } else {
    auto cmd = facade::ToSV(cmd_with_full_args_.front());
    entry_payload = make_pair(cmd, GetShardArgs(shard->shard_id()));
  }
  LogJournalOnShard(shard, std::move(entry_payload), unique_shard_cnt_, false, true);
}

void Transaction::LogJournalOnShard(EngineShard* shard, journal::Entry::Payload&& payload,
                                    uint32_t shard_cnt, bool multi_commands,
                                    bool allow_await) const {
  auto journal = shard->journal();
  CHECK(journal);
  if (multi_)
    multi_->shard_journal_write[shard->shard_id()] = true;

  bool is_multi = multi_commands || IsAtomicMulti();

  auto opcode = is_multi ? journal::Op::MULTI_COMMAND : journal::Op::COMMAND;
  // journal->RecordEntry(txid_, opcode, db_index_, shard_cnt, std::move(payload), allow_await);
}

void Transaction::FinishLogJournalOnShard(EngineShard* shard, uint32_t shard_cnt) const {
  if (multi_) {
    return;
  }
  /*auto journal = shard->journal();
  CHECK(journal);
  journal->RecordEntry(txid_, journal::Op::EXEC, db_index_, shard_cnt, {}, false);*/
}

void Transaction::BreakOnShutdown() {
  if (coordinator_state_ & COORD_BLOCKED) {
    coordinator_state_ |= COORD_CANCELLED;
    blocking_ec_.notify();
  }
}

OpResult<KeyIndex> DetermineKeys(const CommandId* cid, CmdArgList args) {
  if (cid->opt_mask() & CO::GLOBAL_TRANS)
    return KeyIndex::Empty();

  KeyIndex key_index;

  int num_custom_keys = -1;

  if (cid->opt_mask() & CO::VARIADIC_KEYS) {
    // ZUNION/INTER <num_keys> <key1> [<key2> ...]
    // EVAL <script> <num_keys>
    if (args.size() < 3) {
      return OpStatus::SYNTAX_ERR;
    }

    string_view name{cid->name()};

    if (absl::EndsWith(name, "STORE")) {
      key_index.bonus = 1;  // Z<xxx>STORE commands
    }

    unsigned num_keys_index = absl::StartsWith(name, "EVAL") ? 2 : key_index.bonus + 1;

    string_view num = ArgS(args, num_keys_index);
    if (!absl::SimpleAtoi(num, &num_custom_keys) || num_custom_keys < 0)
      return OpStatus::INVALID_INT;

    if (args.size() < size_t(num_custom_keys) + num_keys_index + 1)
      return OpStatus::SYNTAX_ERR;
  }

  if (cid->first_key_pos() > 0) {
    key_index.start = cid->first_key_pos();
    int last = cid->last_key_pos();
    if (num_custom_keys >= 0) {
      key_index.end = key_index.start + num_custom_keys;
    } else {
      key_index.end = last > 0 ? last + 1 : (int(args.size()) + 1 + last);
    }
    key_index.step = cid->key_arg_step();

    return key_index;
  }

  LOG(FATAL) << "TBD: Not supported " << cid->name();

  return key_index;
}

std::vector<Transaction::PerShardCache>& Transaction::TLTmpSpace::GetShardIndex(unsigned size) {
  shard_cache.resize(size);
  for (auto& v : shard_cache)
    v.Clear();
  return shard_cache;
}

}  // namespace dfly
