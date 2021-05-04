#pragma once

#include "../Definitions.h"
#include "Operator.h"

namespace Operator
{
    class _Append : public _TransformingOperator
    {
    public:
        _Append(std::shared_ptr<_Datatype> newDatatype, std::string initializationValue)
            : _newDatatype(newDatatype)
            , _initializationValue(initializationValue)
        {}

        std::string getCode() override;
        std::shared_ptr<_Schema> getOutputSchema() override;
        std::set<std::string> getHeaders() override;

        using BaseType = _TransformingOperator;
    private:
        std::shared_ptr<_Datatype> _newDatatype;
        std::string _initializationValue;
    };

    DECLARE_SHARED_PTR(Append);

    class _AppendTimestamp : public _TransformingOperator
    {
    public:
        _AppendTimestamp(std::shared_ptr<_Datatype> timestamp)
            : _timestamp(timestamp)
        {}

        std::string getCode() override;
        std::shared_ptr<_Schema> getOutputSchema() override;
        std::set<std::string> getHeaders() override;

        using BaseType = _TransformingOperator;
    private:
        std::shared_ptr<_Datatype> _timestamp;
    };

    DECLARE_SHARED_PTR(AppendTimestamp);
}
