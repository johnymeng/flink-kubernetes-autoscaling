#include "shards_estimator.h"

#include <array>
#include <iostream>
#include <thread>

namespace {
uint64_t TestHash(const char* key, size_t size, uint64_t seed) {
  return size > 0 && key[0] == 'b' ? UINT64_MAX : seed;
}
}  // namespace

int main() {
  using a4s::ShardsEstimator;
  ShardsEstimator estimator(0.5, 17, 10, 30, TestHash);
  estimator.Access("b", 1, 10);
  estimator.Access("c", 1, 10, true);
  estimator.Access("a", 1, 10);

  std::array<std::thread, 4> threads;
  for (auto& thread : threads) {
    thread = std::thread([&estimator] {
      for (int i = 0; i < 1000; ++i) {
        estimator.Access("a", 1, 10, true);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  const auto result = estimator.Snapshot();
  size_t first_bin = ShardsEstimator::kHeaderSize;
  if (result.size() != first_bin + 4 ||
      result[ShardsEstimator::kSampledRefs] != 4002 ||
      result[ShardsEstimator::kTotalRefs] != 4003 ||
      result[ShardsEstimator::kTrackedEntries] != 1 ||
      result[first_bin] != 4000 || result[first_bin + 3] != 2) {
    std::cerr << "Unexpected SHARDS snapshot" << std::endl;
    return 1;
  }
  return 0;
}
