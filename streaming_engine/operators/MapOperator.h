#pragma once

#include "Definitions.h"
#include "TransformingOperator.h"

template <typename InputType, typename OutputType>
class _MapOperator : public _TransformingOperator<InputType, OutputType> {
public:
    explicit _MapOperator(std::function<OutputType(InputType)> mapFunction) : _mapFunction(mapFunction) {}

    OutputType execute() override {
        return _mapFunction(this->executePreviousOperator());
    };

private:
    std::function<OutputType(InputType)> _mapFunction;
};

DECLARE_SHARED_PTR(MapOperator);
