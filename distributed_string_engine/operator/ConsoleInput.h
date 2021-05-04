#pragma once

#include <string>

#include "../Definitions.h"
#include "../Schema.h"
#include "Operator.h"

namespace Operator
{
    class _ConsoleInput : public _ProducingOperator
    {
    public:
        std::string getCode() override;
        std::shared_ptr<_Schema> getOutputSchema() override;
        std::set<std::string> getHeaders() override;

        using BaseType = _ProducingOperator;
    };

    DECLARE_SHARED_PTR(ConsoleInput);
}
