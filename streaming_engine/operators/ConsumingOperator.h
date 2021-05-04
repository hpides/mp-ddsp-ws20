#pragma once

#include <algorithm>

#include "GeneratingOperator.h"

template<typename InputType>
class _ConsumingOperator
{
public:
    using BaseType = _ConsumingOperator;
    void setPreviousOperator(std::shared_ptr<_GeneratingOperator<InputType>> previousOperator) { _previousOperator = previousOperator; };
    InputType executePreviousOperator() { return _previousOperator->execute(); }
    virtual void executePipeline(){};

protected:
    std::shared_ptr<_GeneratingOperator<InputType>> _previousOperator;
};
