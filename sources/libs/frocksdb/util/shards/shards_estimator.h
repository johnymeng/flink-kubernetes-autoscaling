#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace a4s {

// Online fixed-rate SHARDS miss-ratio-curve estimator with byte distances.
//
// Access() is fed one block access at a time. A block is sampled if
// hash(block_key, hash_seed) <= sampling_rate * 2^64, the same
// spatial sampling as the offline ShardsMRC. For a re-access of a sampled
// block, the distance is the sum of the charges of the distinct sampled blocks
// accessed since its previous access, divided by sampling_rate, plus its own
// charge. With ordinary cache fills this estimates the smallest byte-LRU cache
// in which the access hits. Bin i counts distances in
// (i * bin_bytes, (i + 1) * bin_bytes], i.e. accesses that hit in
// a cache of (i + 1) * bin_bytes bytes, with the final bin limited to
// max_distance_bytes. One infinite bin follows. It holds first accesses,
// re-accesses of blocks evicted by the bound, and distances beyond the cap.
// Lookup-only reads leave cold blocks out. A modeled hit refreshes the block
// in the shared recency order. Smaller caches can have a different order if
// they missed that lookup. With mixed fills a smaller cache can even hit more
// often. Use shards_trace_replay --capacities for independent per-size counts.
//
// The sample set is bounded by dropping the least recently used sampled
// block while its estimated hit size exceeds max_distance_bytes. Each drop
// contributes to evicted_by_cap.
//
// All counters are monotonic and never reset. Thread-safe; the hash is
// computed before the mutex, which is taken only for sampled accesses.
class ShardsEstimator {
 public:
  // Layout of Snapshot(). Histogram counts are unscaled sampled references.
  enum SnapshotField : size_t {
    kSamplingRateBits = 0,  // IEEE-754 bits of sampling_rate
    kHashSeed,
    kBinBytes,
    kMaxDistanceBytes,
    kTotalRefs,       // all accesses passed to Access()
    kSampledRefs,     // accesses that passed the hash test
    kEvictedByCap,    // sampled blocks dropped by the distance bound
    kTrackedEntries,  // sampled blocks currently tracked
    kTrackedBytes,    // sum of their charges (unscaled)
    kEstimatorNanos,  // time spent updating for sampled accesses
    kHeaderSize,      // num_bins finite bins follow, then the infinite bin
  };

  // The caller supplies a stable byte key scoped to the cache and a seeded,
  // deterministic hash. Until Access returns the key view must remain valid.
  using Hash = uint64_t (*)(const char*, size_t, uint64_t);

  // Requires 0 < sampling_rate <= 1, bin_bytes > 0, max_distance_bytes > 0.
  ShardsEstimator(double sampling_rate, uint64_t hash_seed,
                     uint64_t bin_bytes, uint64_t max_distance_bytes, Hash hash);

  ShardsEstimator(const ShardsEstimator&) = delete;
  ShardsEstimator& operator=(const ShardsEstimator&) = delete;

  // A lookup that cannot insert still counts as a reference. It can refresh
  // an already tracked block, using the charge from its admission.
  void Access(const char* key, size_t key_size, uint64_t charge,
              bool no_insert = false);

  // Header (see SnapshotField) followed by num_bins() + 1 histogram counts.
  std::vector<uint64_t> Snapshot() const;

  uint64_t num_bins() const { return num_bins_; }

  // Histogram bin of a distance (bin i covers (i, i + 1] * bin_bytes).
  static uint64_t BinOf(uint64_t distance, uint64_t bin_bytes);

 private:
  struct TrackedBlock {
    uint64_t slot = 0;
    // A lookup-only hit cannot warm a cache too small for that hit. Keep
    // its minimum hit bin until an insertion-permitting access to the block.
    uint64_t min_hit_bin = 0;
  };
  using KeyMap = std::unordered_map<std::string, TrackedBlock>;

  uint64_t HitBin(uint64_t newer, uint64_t charge) const;
  void RecordSample(const char* key, size_t key_size, uint64_t charge,
                    bool no_insert);

  // Fenwick tree over access slots in recency order; a slot holds the charge
  // of the block whose most recent access it is.
  void FenwickAdd(uint64_t slot, uint64_t delta);
  uint64_t FenwickPrefix(uint64_t slot) const;  // sum of slots [0, slot]
  void RemoveSlot(uint64_t slot);
  // Renumbers live slots to [0, n) when the slot array is full.
  void Compact();

  const double sampling_rate_;
  const uint64_t threshold_;
  const uint64_t hash_seed_;
  const uint64_t bin_bytes_;
  const uint64_t max_distance_bytes_;
  const uint64_t num_bins_;

  const Hash hash_;
  std::atomic<uint64_t> total_refs_{0};

  mutable std::mutex mutex_;
  uint64_t sampled_refs_ = 0;
  uint64_t evicted_by_cap_ = 0;
  uint64_t tracked_bytes_ = 0;
  uint64_t estimator_nanos_ = 0;
  std::vector<uint64_t> histogram_;

  KeyMap blocks_;
  std::vector<uint64_t> fenwick_;
  std::vector<uint64_t> slot_charge_;
  std::vector<KeyMap::value_type*> slot_block_;  // nullptr if slot is free
  uint64_t oldest_slot_ = 0;
  uint64_t next_slot_ = 0;
};

}  // namespace a4s
