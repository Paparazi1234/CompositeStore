#include "system_clock.h"

#include "port/likely.h"

namespace MULTI_VERSIONS_NAMESPACE {

Status SystemClock::GetCurrentTime(int64_t* unix_time){
  time_t ret = time(nullptr);
  if (ret == (time_t)-1) {
    return Status::NotSupported();
  }
  *unix_time = (int64_t)ret;
  return Status::OK();
}

std::string SystemClock::TimeToString(uint64_t secondsSince1970) {
  const time_t seconds = (time_t)secondsSince1970;
  struct tm t;
  int maxsize = 64;
  std::string dummy;
  dummy.reserve(maxsize);
  dummy.resize(maxsize);
  char* p = &dummy[0];
  localtime_r(&seconds, &t);
  snprintf(p, maxsize, "%04d/%02d/%02d-%02d:%02d:%02d ", t.tm_year + 1900,
           t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec);
  return dummy;
}

SystemClock* SystemClock::GetTLSInstance() {
  static thread_local SystemClock* tls_instance;
  static thread_local std::aligned_storage<sizeof(SystemClock)>::type 
      tls_instance_bytes;

  auto rv = tls_instance;
  if (UNLIKELY(rv == nullptr)) {
    rv = new (&tls_instance_bytes) SystemClock();
    tls_instance = rv;
  }
  return rv;
}

}   // namespace MULTI_VERSIONS_NAMESPACE
