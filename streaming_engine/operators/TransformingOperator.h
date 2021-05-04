#pragma once

#include "GeneratingOperator.h"
#include "ConsumingOperator.h"

// TODO: Find better name (a filter operator is also a TransformingOperator)
template<typename InputType, typename OutputType>
class _TransformingOperator
    : public _ConsumingOperator<InputType>
        , public _GeneratingOperator<OutputType>
{
public:
    using BaseType = _TransformingOperator;
};
