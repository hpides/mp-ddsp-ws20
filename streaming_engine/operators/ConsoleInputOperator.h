#pragma once

#include <iostream>

#include "Definitions.h"
#include "GeneratingOperator.h"

template<typename OutputType>
class _ConsoleInputOperator : public _GeneratingOperator<OutputType>
{
public:
    OutputType execute() override
    {
        OutputType input;
        std::cin >> input;
        return input;
    }
};

DECLARE_SHARED_PTR(ConsoleInputOperator);
