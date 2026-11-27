#pragma once

#include "rocksdb/slice.h"
#include "util/shards/shards_estimator.h"

namespace ROCKSDB_NAMESPACE {

// Extends the cache-local estimator with RocksDB's stable key and exact sampling hash.
class ShardsMrcEstimator : public a4s::ShardsEstimator {
 public:
  ShardsMrcEstimator(double sampling_rate, uint64_t hash_seed,
                     uint64_t bin_bytes, uint64_t max_distance_bytes);

  void Access(const Slice& block_key, uint64_t charge, bool no_insert = false) {
    ShardsEstimator::Access(block_key.data(), block_key.size(), charge, no_insert);
  }
};

}  // namespace ROCKSDB_NAMESPACE
