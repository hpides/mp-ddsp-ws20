#include <chrono>

inline uint64_t getCurrentUnixTimestampInMilliseconds()
{
    const auto t = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch()).count();
}
