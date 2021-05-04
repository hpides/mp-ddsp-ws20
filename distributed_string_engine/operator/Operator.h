#pragma once

#include <set>
#include <string>

#include "../Schema.h"

namespace Operator
{
    class _Operator
    {
    public:
        virtual ~_Operator() = default;

        virtual std::string getCode() = 0;
        virtual std::set<std::string> getHeaders() { return {}; };
        virtual std::string getGlobalDeclarations() { return ""; };
        virtual bool isPipelineBreaker() { return false; };
    };

    class _ConsumingOperator : virtual public _Operator
    {
    public:
        virtual void setInputSchema(std::shared_ptr<_Schema> schema) { _inputSchema = schema; };

    protected:
        std::shared_ptr<_Schema> _inputSchema;
    };

    class _ProducingOperator : virtual public _Operator
    {
    public:
        virtual std::shared_ptr<_Schema> getOutputSchema() = 0;
    };

    class _TransformingOperator
        : virtual public _ConsumingOperator
        , virtual public _ProducingOperator
    {};

    class _IdentityOperator : public _TransformingOperator
    {
        std::shared_ptr<_Schema> getOutputSchema() { return _inputSchema; };
    };

    class _PipelineBreaker : public _TransformingOperator
    {
    public:
        virtual std::string getNewThreadCode() = 0;
        bool isPipelineBreaker() override { return true; }
    };
}
