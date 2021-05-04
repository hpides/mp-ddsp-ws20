#pragma once

#include <memory>

#include "Pipeline.h"
#include "operator/Operator.h"

static std::shared_ptr<_Pipeline> operator|(
    std::shared_ptr<Operator::_ProducingOperator> leftOp,
    std::shared_ptr<Operator::_TransformingOperator> rightOp) {
    auto pipe = Pipeline();
    pipe->add(leftOp);
    pipe->add(rightOp);
    return pipe;
}

static void operator|(std::shared_ptr<Operator::_ProducingOperator> leftOp, std::shared_ptr<Operator::_ConsumingOperator> rightOp) {
    auto pipe = Pipeline();
    pipe->add(leftOp);
    pipe->add(rightOp);
    pipe->compile();
}

static std::shared_ptr<_Pipeline> operator|(std::shared_ptr<_Pipeline> pipe, std::shared_ptr<Operator::_TransformingOperator> op) {
    pipe->add(op);
    return pipe;
}

static void operator|(std::shared_ptr<_Pipeline> pipe, std::shared_ptr<Operator::_ConsumingOperator> op) {
    pipe->add(op);
    pipe->compile();
}
