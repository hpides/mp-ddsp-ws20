#pragma once

#include "Definitions.h"
#include "GeneratingOperator.h"
#include <queue>

template<typename OutputType>
class _ConstantInputOperator : public _GeneratingOperator<OutputType>
{
public:
    explicit _ConstantInputOperator(std::queue<OutputType> dataQueue) : _dataQueue(dataQueue) {}

    OutputType execute() override {
        OutputType value = _dataQueue.front();
        _dataQueue.pop();
        return value;
    }

private:
    std::queue<OutputType> _dataQueue;
};

DECLARE_SHARED_PTR(ConstantInputOperator);
