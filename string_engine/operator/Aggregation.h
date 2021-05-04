#pragma once

#include <string>

#include "../Definitions.h"
#include "../Schema.h"
#include "Operator.h"

namespace Operator
{

    class _Aggregation : public _TransformingOperator
    {
    public:
        explicit _Aggregation(std::shared_ptr<_AggregationSchema> schema)
            : _aggregationSchema(schema){};

        std::string getCode() override;
        std::set<std::string> getHeaders() override;
        void setInputSchema(std::shared_ptr<_Schema> inputSchema) override;
        std::shared_ptr<_Schema> getOutputSchema() override;

        using BaseType = _TransformingOperator;

    private:
        std::shared_ptr<_Schema> _outputSchema;
        std::shared_ptr<_AggregationSchema> _aggregationSchema;
        std::shared_ptr<_Datatype> _queueType;
        uint32_t _keyIndex;

        static std::string getValueByIndex(uint index, std::string tupleName = "tuple");
        static std::string getElementAggregation(uint index, _AggregationSchema::AggregationType type);
    };

    DECLARE_SHARED_PTR(Aggregation);
}
