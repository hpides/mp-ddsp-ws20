#pragma once

#include <sstream>
#include <utility>

#include "../Definitions.h"
#include "Operator.h"
#include "fmt/core.h"

namespace Operator
{
    class _Filter : public _IdentityOperator
    {
    public:
        /**
         *
         * @param filterExpression tuples matching that expression are skipped
         */
        _Filter(std::shared_ptr<_Datatype> filterDatatype, std::string filterExpression)
            : _filterDatatype(filterDatatype), _filterExpression(std::move(filterExpression)){};

        std::string getCode() override;

        using BaseType = _TransformingOperator;

    private:
        std::string _filterExpression;
        std::shared_ptr<_Datatype> _filterDatatype;
    };

    DECLARE_SHARED_PTR(Filter);
}
