#include "system_clock.h"

namespace MULTI_VERSIONS_NAMESPACE {

uint64_t SystemClock::NowMicros() {
  return 0;
}

void SystemClock::SleepForMicroseconds(int micros) {

}

Status SystemClock::GetCurrentTime(int64_t* unix_time){
  return Status::OK();
}

std::string SystemClock::TimeToString(uint64_t time) {
  return "";
}

}   // namespace MULTI_VERSIONS_NAMESPACE
