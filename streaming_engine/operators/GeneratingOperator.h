#pragma once

template<typename OutputType>
class _GeneratingOperator
{
public:
    using BaseType = _GeneratingOperator;
    virtual OutputType execute() = 0;
};
