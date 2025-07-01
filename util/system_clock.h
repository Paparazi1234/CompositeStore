#pragma once

#include <stdint.h>
#include <string>

#include "include/status.h"

namespace MULTI_VERSIONS_NAMESPACE {

class SystemClock {
 public:
  // Returns the number of micro-seconds since some fixed point in time.
  // It is often used as system time such as in GenericRateLimiter
  // and other places so a port needs to return system time in order to work.
  uint64_t NowMicros();

  // Returns the number of nano-seconds since some fixed point in time. Only
  // useful for computing deltas of time in one run.
  // Default implementation simply relies on NowMicros.
  // In platform-specific implementations, NowNanos() should return time points
  // that are MONOTONIC.
  uint64_t NowNanos() { return NowMicros() * 1000; }

  // Returns the number of micro-seconds of CPU time used by the current thread.
  // 0 indicates not supported.
  uint64_t CPUMicros() { return 0; }

  // Returns the number of nano-seconds of CPU time used by the current thread.
  // Default implementation simply relies on CPUMicros.
  // 0 indicates not supported.
  uint64_t CPUNanos() { return CPUMicros() * 1000; }

  // Sleep/delay the thread for the prescribed number of micro-seconds.
  void SleepForMicroseconds(int micros);

  // Get the number of seconds since the Epoch, 1970-01-01 00:00:00 (UTC).
  // Only overwrites *unix_time on success.
  Status GetCurrentTime(int64_t* unix_time);

  // Converts seconds-since-Jan-01-1970 to a printable string
  std::string TimeToString(uint64_t time);
};

}   // namespace MULTI_VERSIONS_NAMESPACE
