#pragma once

#include <chrono>
#include <deque>
#include <mutex>
#include <thread>

#include "Definitions.h"
#include "TransformingOperator.h"

template<typename ContainerType>
class _WindowOperator : public _TransformingOperator<typename ContainerType::value_type, ContainerType>
{
public:
    _WindowOperator(uint64_t windowSizeMillis, uint64_t windowSlideSizeMillis)
        : _windowSlideSizeMillis(windowSlideSizeMillis)
        , _currentSlideStartedTime(std::chrono::high_resolution_clock::now())
        , _isCollecting(false)
    {
        if (windowSizeMillis % _windowSlideSizeMillis.count() != 0)
            throw std::runtime_error("WindowSize must be a multiple of windowSlide");

        _numWindowSlides = static_cast<typename decltype(_slideContainers)::size_type>(windowSizeMillis / windowSlideSizeMillis);
    }

    ContainerType execute() override
    {

        // Add a new slide
        _slideContainers.emplace_back();
        if (_isError)
            _emptyWindows++;

        // In a separate thread, collect elements from the pipeline into the new slide
        _isCollecting = true;
        std::thread([&] {
            while (_isCollecting) {
                try {
                    auto currentElement = this->executePreviousOperator();
                    {
                        std::lock_guard lock(_bufferMutex);
                        _slideContainers.back().push_back(currentElement);
                    }
                } catch (...) {
                    _isCollecting = false;
                    _isError = true;
                }
            }
        }).detach();

        // In main thread, wait until current slide is over
        _currentSlideStartedTime += _windowSlideSizeMillis;
        std::this_thread::sleep_until(std::chrono::time_point(_currentSlideStartedTime));
        _isCollecting = false;
        std::lock_guard lock(_bufferMutex);

        // Collect all slides of windowSize to a result container
        ContainerType resultContainer;
        for (int i = 0; i < std::min(_numWindowSlides, _slideContainers.size()); i++) {
            resultContainer.insert(resultContainer.end(), _slideContainers[i].begin(), _slideContainers[i].end());
        }

        // Remove old slide (if not belonging to next window anymore)
        while (_slideContainers.size() >= _numWindowSlides)
            _slideContainers.pop_front();

        if (_emptyWindows >= _numWindowSlides){
            std::cout << std::flush;
            throw std::runtime_error("End of pipeline");
        }

        return resultContainer;
    }

private:
    std::chrono::milliseconds _windowSlideSizeMillis;
    std::chrono::time_point<std::chrono::high_resolution_clock> _currentSlideStartedTime;
    std::deque<ContainerType> _slideContainers;
    typename decltype(_slideContainers)::size_type _numWindowSlides;
    std::mutex _bufferMutex;
    bool _isCollecting;
    bool _isError = false;
    int _emptyWindows = 0;
};
DECLARE_SHARED_PTR(WindowOperator);
