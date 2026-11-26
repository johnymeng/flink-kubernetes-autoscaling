#include "cache/shards_mrc_estimator.h"

#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

namespace {
uint64_t HashCacheKey(const char* key, size_t size, uint64_t seed) {
  return GetSliceNPHash64(Slice(key, size), seed);
}
}  // namespace

ShardsMrcEstimator::ShardsMrcEstimator(double sampling_rate, uint64_t hash_seed,
                                       uint64_t bin_bytes,
                                       uint64_t max_distance_bytes)
    : ShardsEstimator(sampling_rate, hash_seed, bin_bytes, max_distance_bytes,
                      HashCacheKey) {}

}  // namespace ROCKSDB_NAMESPACE
