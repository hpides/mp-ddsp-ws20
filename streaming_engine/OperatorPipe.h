#pragma once

#include <future>
#include <memory>

#include "operators/ConsumingOperator.h"
#include "operators/GeneratingOperator.h"
#include "operators/TransformingOperator.h"

template<typename InputType, typename OutputType>
std::shared_ptr<_TransformingOperator<InputType, OutputType>> operator|(
    std::shared_ptr<_GeneratingOperator<InputType>> generatingOperator,
    std::shared_ptr<_TransformingOperator<InputType, OutputType>> op2)
{
    op2->setPreviousOperator(generatingOperator);
    return op2;
}

template<typename InputType, typename CommonType, typename OutputType>
std::shared_ptr<_TransformingOperator<CommonType, OutputType>> operator|(
    std::shared_ptr<_TransformingOperator<InputType, CommonType>> op1,
    std::shared_ptr<_TransformingOperator<CommonType, OutputType>> op2)
{
    op2->setPreviousOperator(op1);
    return op2;
}

template<typename InputType, typename OutputType>
auto operator|(
    std::shared_ptr<_TransformingOperator<InputType, OutputType>> op1,
    std::shared_ptr<_ConsumingOperator<OutputType>> consumingOperator)
{
    consumingOperator->setPreviousOperator(op1);
    //try {
        consumingOperator->executePipeline();
    //} catch (...) {}
    //return std::async(std::launch::async, [consumingOperator]() { consumingOperator->executePipeline(); });
}
