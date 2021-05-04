#pragma once

#include "Definitions.h"
#include "TransformingOperator.h"

template<typename ElementType>
class _FilterOperator : public _TransformingOperator<ElementType, ElementType>
{
public:
    explicit _FilterOperator(std::function<bool(ElementType)> predicate)
        : _predicate(predicate)
    {}

    ElementType execute() override
    {
        while (true)
            if (auto res = this->executePreviousOperator(); _predicate(res))
                return res;
    }

private:
    std::function<bool(ElementType)> _predicate;
};

DECLARE_SHARED_PTR(FilterOperator);
