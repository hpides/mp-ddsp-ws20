#pragma once

#include <sstream>
#include <vector>

#include "../Definitions.h"
#include "../Schema.h"
#include "Operator.h"
#include "fmt/core.h"

namespace Operator
{
    class _Parse : public _TransformingOperator
    {
    public:
        explicit _Parse(std::shared_ptr<_Schema> schema)
            : _schema(schema){};

        std::string getCode() override;

        std::set<std::string> getHeaders() override;

        std::shared_ptr<_Schema> getOutputSchema() override {
            return _schema;
        }

        using BaseType = _TransformingOperator;

    private:
        static std::string parseIntegral();
        static std::string parseFloatingPoint();
        static std::string parse(std::string whileCondition, std::string posName, std::string variableType, std::string variableName);
        std::shared_ptr<_Schema> _schema;
    };

    DECLARE_SHARED_PTR(Parse);
}
