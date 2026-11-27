#include "shards_estimator.h"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstring>
#include <limits>
#include <utility>

namespace a4s {

namespace {
constexpr uint64_t kMinSlots = 1024;
}  // namespace

uint64_t ShardsEstimator::BinOf(uint64_t distance, uint64_t bin_bytes) {
  return distance == 0 ? 0 : (distance - 1) / bin_bytes;
}

uint64_t ShardsEstimator::HitBin(uint64_t newer, uint64_t charge) const {
  if (charge > max_distance_bytes_) {
    return num_bins_;
  }
  uint64_t newer_bytes = newer;
  if (sampling_rate_ < 1.0) {
    const double scaled = static_cast<double>(newer) / sampling_rate_;
    // The conversion truncates to bytes. Check the range before converting.
    if (scaled >= std::ldexp(1.0, std::numeric_limits<uint64_t>::digits)) {
      return num_bins_;
    }
    newer_bytes = static_cast<uint64_t>(scaled);
  }
  if (newer_bytes > max_distance_bytes_ - charge) {
    return num_bins_;
  }
  return BinOf(newer_bytes + charge, bin_bytes_);
}

ShardsEstimator::ShardsEstimator(double sampling_rate, uint64_t hash_seed,
                                       uint64_t bin_bytes,
                                       uint64_t max_distance_bytes, Hash hash)
    : sampling_rate_(sampling_rate),
      // Same threshold as ShardsMRC, so both select the same blocks.
      threshold_(
          sampling_rate >= 1.0
              ? std::numeric_limits<uint64_t>::max()
              : static_cast<uint64_t>(
                    sampling_rate *
                    static_cast<double>(std::numeric_limits<uint64_t>::max()))),
      hash_seed_(hash_seed),
      bin_bytes_(bin_bytes),
      max_distance_bytes_(max_distance_bytes),
      num_bins_((max_distance_bytes + bin_bytes - 1) / bin_bytes),
      hash_(hash),
      histogram_(num_bins_ + 1, 0) {
  assert(sampling_rate > 0.0 && sampling_rate <= 1.0);
  assert(bin_bytes > 0 && max_distance_bytes > 0 && hash != nullptr);
}

void ShardsEstimator::Access(const char* key, size_t key_size, uint64_t charge,
                            bool no_insert) {
  if (hash_(key, key_size, hash_seed_) > threshold_) {
    total_refs_.fetch_add(1, std::memory_order_relaxed);
    return;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  const auto start = std::chrono::steady_clock::now();
  total_refs_.fetch_add(1, std::memory_order_relaxed);
  ++sampled_refs_;
  RecordSample(key, key_size, charge, no_insert);
  estimator_nanos_ += static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - start)
          .count());
}

void ShardsEstimator::RecordSample(const char* data, size_t key_size,
                                  uint64_t charge, bool no_insert) {
  std::string key(data, key_size);
  auto it = blocks_.find(key);
  if (it == blocks_.end()) {
    ++histogram_[num_bins_];
    if (no_insert) {
      return;
    }
    it = blocks_.emplace(std::move(key), TrackedBlock{}).first;
  } else {
    // Byte-LRU stack distance: the block hits in a cache of at least this many
    // bytes. Blocks accessed since its last access (the slots after its slot)
    // are a 1/R sample, so scale them; its own charge is exact.
    const uint64_t old_slot = it->second.slot;
    if (no_insert) {
      // A miss in the observed cache can have no measured charge even when
      // a larger modeled cache would still hold the block.
      charge = slot_charge_[old_slot];
    }
    const uint64_t newer = tracked_bytes_ - FenwickPrefix(old_slot);
    const uint64_t bin =
        std::max(HitBin(newer, charge), it->second.min_hit_bin);
    ++histogram_[bin];
    it->second.min_hit_bin = no_insert ? bin : 0;
    RemoveSlot(old_slot);
  }

  if (next_slot_ == slot_block_.size()) {
    Compact();
  }
  const uint64_t slot = next_slot_++;
  it->second.slot = slot;
  slot_block_[slot] = &*it;
  slot_charge_[slot] = charge;
  FenwickAdd(slot, charge);
  tracked_bytes_ += charge;

  while (!blocks_.empty()) {
    while (slot_block_[oldest_slot_] == nullptr) {
      ++oldest_slot_;
    }
    const uint64_t oldest_charge = slot_charge_[oldest_slot_];
    if (HitBin(tracked_bytes_ - oldest_charge, oldest_charge) != num_bins_) {
      break;
    }
    KeyMap::value_type* victim = slot_block_[oldest_slot_];
    RemoveSlot(oldest_slot_);
    blocks_.erase(blocks_.find(victim->first));
    ++evicted_by_cap_;
  }
}

std::vector<uint64_t> ShardsEstimator::Snapshot() const {
  std::vector<uint64_t> out(kHeaderSize);
  std::memcpy(&out[kSamplingRateBits], &sampling_rate_, sizeof(uint64_t));
  out[kHashSeed] = hash_seed_;
  out[kBinBytes] = bin_bytes_;
  out[kMaxDistanceBytes] = max_distance_bytes_;
  std::lock_guard<std::mutex> lock(mutex_);
  // Publish sampled accesses and their contribution to total_refs_ together.
  out[kTotalRefs] = total_refs_.load(std::memory_order_relaxed);
  out[kSampledRefs] = sampled_refs_;
  out[kEvictedByCap] = evicted_by_cap_;
  out[kTrackedEntries] = blocks_.size();
  out[kTrackedBytes] = tracked_bytes_;
  out[kEstimatorNanos] = estimator_nanos_;
  out.insert(out.end(), histogram_.begin(), histogram_.end());
  return out;
}

void ShardsEstimator::FenwickAdd(uint64_t slot, uint64_t delta) {
  // Unsigned wrap-around makes adding (0 - charge) a subtraction.
  for (uint64_t i = slot + 1; i < fenwick_.size(); i += i & (0 - i)) {
    fenwick_[i] += delta;
  }
}

uint64_t ShardsEstimator::FenwickPrefix(uint64_t slot) const {
  uint64_t sum = 0;
  for (uint64_t i = slot + 1; i > 0; i -= i & (0 - i)) {
    sum += fenwick_[i];
  }
  return sum;
}

void ShardsEstimator::RemoveSlot(uint64_t slot) {
  FenwickAdd(slot, 0 - slot_charge_[slot]);
  tracked_bytes_ -= slot_charge_[slot];
  slot_charge_[slot] = 0;
  slot_block_[slot] = nullptr;
}

void ShardsEstimator::Compact() {
  std::vector<KeyMap::value_type*> blocks;
  std::vector<uint64_t> charges;
  for (uint64_t s = oldest_slot_; s < next_slot_; ++s) {
    if (slot_block_[s] != nullptr) {
      slot_block_[s]->second.slot = blocks.size();
      blocks.push_back(slot_block_[s]);
      charges.push_back(slot_charge_[s]);
    }
  }
  const uint64_t capacity =
      std::max<uint64_t>(kMinSlots, 2 * blocks.size() + 2);
  blocks.resize(capacity, nullptr);
  charges.resize(capacity, 0);
  slot_block_.swap(blocks);
  slot_charge_.swap(charges);
  // Linear-time Fenwick build.
  fenwick_.assign(capacity + 1, 0);
  for (uint64_t i = 1; i <= capacity; ++i) {
    fenwick_[i] += slot_charge_[i - 1];
    const uint64_t parent = i + (i & (0 - i));
    if (parent <= capacity) {
      fenwick_[parent] += fenwick_[i];
    }
  }
  next_slot_ = 0;
  while (next_slot_ < capacity && slot_block_[next_slot_] != nullptr) {
    ++next_slot_;
  }
  oldest_slot_ = 0;
}

}  // namespace a4s
