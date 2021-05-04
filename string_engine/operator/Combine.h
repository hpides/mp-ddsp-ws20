#pragma once

#include "../Definitions.h"
#include "../Schema.h"
#include "Operator.h"

namespace Operator
{
    class _Combine : public _TransformingOperator
    {
    public:
        _Combine(
            std::shared_ptr<_Datatype> newDatatype,
            std::shared_ptr<_Datatype> leftDatatype,
            std::shared_ptr<_Datatype> rightDatatype,
            std::shared_ptr<AggregationStrategy::_AggregationStrategy> aggregationStrategy)
            : _newDatatype(newDatatype)
            , _leftDatatype(leftDatatype)
            , _rightDatatype(rightDatatype)
            , _aggregationStrategy(aggregationStrategy)
        {}

        std::string getCode() override;
        std::shared_ptr<_Schema> getOutputSchema() override;
        std::set<std::string> getHeaders() override;

        using BaseType = _TransformingOperator;
    private:
        std::shared_ptr<_Datatype> _newDatatype, _leftDatatype, _rightDatatype;
        std::shared_ptr<AggregationStrategy::_AggregationStrategy> _aggregationStrategy;
    };

    DECLARE_SHARED_PTR(Combine);
}
