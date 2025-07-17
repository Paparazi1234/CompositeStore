#pragma once

#include <stdint.h>
#include <string>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

// Get nano time includes
#if defined(OS_LINUX) || defined(OS_FREEBSD) || defined(OS_GNU_KFREEBSD)
#elif defined(__MACH__)
#include <Availability.h>
#include <mach/clock.h>
#include <mach/mach.h>
#else
#include <chrono>
#endif

#include "include/status.h"

namespace COMPOSITE_STORE_NAMESPACE {

class SystemClock {
 public:
  // Returns the number of micro-seconds since some fixed point in time.
  // It is often used as system time such as in GenericRateLimiter
  // and other places so a port needs to return system time in order to work.
  uint64_t NowMicros() {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
  }

  // Returns the number of nano-seconds since some fixed point in time. Only
  // useful for computing deltas of time in one run.
  // In platform-specific implementations, NowNanos() should return time points
  // that are MONOTONIC.
  uint64_t NowNanos() {
#if defined(OS_LINUX) || defined(OS_FREEBSD) || defined(OS_GNU_KFREEBSD)
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000 + ts.tv_nsec;
#elif defined(__MACH__)
    clock_serv_t cclock;
    mach_timespec_t ts;
    host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
    clock_get_time(cclock, &ts);
    mach_port_deallocate(mach_task_self(), cclock);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000 + ts.tv_nsec;
#else
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
#endif
  }

  // Returns the number of micro-seconds of CPU time used by the current thread.
  // 0 indicates not supported.
  uint64_t CPUMicros() {
#if defined(OS_LINUX) || defined(OS_FREEBSD) || defined(OS_GNU_KFREEBSD) || \
    (defined(__MACH__) && defined(__MAC_10_12))
    struct timespec ts;
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
    return (static_cast<uint64_t>(ts.tv_sec) * 1000000000 + ts.tv_nsec) / 1000;
#endif
    return 0;
  }

  // Returns the number of nano-seconds of CPU time used by the current thread.
  // 0 indicates not supported.
  uint64_t CPUNanos() {
#if defined(OS_LINUX) || defined(OS_FREEBSD) || defined(OS_GNU_KFREEBSD) || \
    (defined(__MACH__) && defined(__MAC_10_12))
    struct timespec ts;
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000 + ts.tv_nsec;
#endif
    return 0;
  }

  // Sleep/delay the thread for the prescribed number of micro-seconds.
  void SleepForMicroseconds(int micros) {
    usleep(micros);
  }

  // Get the number of seconds since the Epoch, 1970-01-01 00:00:00 (UTC).
  // Only overwrites *unix_time on success.
  Status GetCurrentTime(int64_t* unix_time);

  // Converts seconds-since-Jan-01-1970 to a printable string
  std::string TimeToString(uint64_t secondsSince1970);

  static SystemClock* GetSingleton();

 private:
  // for singleton
  SystemClock() {}
};

}   // namespace COMPOSITE_STORE_NAMESPACE
