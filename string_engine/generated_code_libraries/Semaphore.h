#include <condition_variable>
#include <mutex>

class Semaphore
{
public:
    void notify()
    {
        std::lock_guard<decltype(_mutex)> lock(_mutex);
        ++_count;
        _condition.notify_one();
    }

    void wait()
    {
        std::unique_lock<decltype(_mutex)> lock(_mutex);
        while (!_count)  // Handle spurious wake-ups.
            _condition.wait(lock);
        --_count;
    }
private:
    std::mutex _mutex;
    std::condition_variable _condition;
    unsigned long _count = 0;
};
