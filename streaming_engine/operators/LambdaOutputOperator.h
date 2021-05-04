#pragma once

#include <queue>

#include "ConsumingOperator.h"
#include "Definitions.h"

template<typename InputType>
class _LambdaOutputOperator : public _ConsumingOperator<InputType>
{
public:
    explicit _LambdaOutputOperator(int pullCount, std::function<void(int, InputType)> assertFunction)
        : _pullCount(pullCount)
        , _assertFunction(assertFunction)
    {}

    void executePipeline()
    {
        int iterationCount = 0;
        while (iterationCount++ < _pullCount) {
            InputType res = this->executePreviousOperator();
            _assertFunction(iterationCount, res);
        }
    }

private:
    int _pullCount;
    std::function<void(int, InputType)> _assertFunction;
};

DECLARE_SHARED_PTR(LambdaOutputOperator);
