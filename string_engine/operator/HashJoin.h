#pragma once

#include "../Definitions.h"
#include "../OperatorPipe.h"
#include "../Pipeline.h"
#include "../Schema.h"
#include "Operator.h"

namespace Operator
{
    class _HashJoinInputAdapter : public _TransformingOperator
    {
    public:
        _HashJoinInputAdapter(std::shared_ptr<_Datatype> semaphore)
            : _semaphore(semaphore)
        {}

        std::string getGlobalDeclarations() override;
        std::string getCode() override;

        void setInputSchema(std::shared_ptr<_Schema> _schema) override
        {
            _inputSchema = _schema;
            // Create a copied map with same signature
            _outputSchema = Schema(_inputSchema->getOnly()->clone());
        }

        std::shared_ptr<_Schema> getOutputSchema() override { return _outputSchema; }

        using BaseType = _TransformingOperator;

    private:
        std::shared_ptr<_Schema> _outputSchema;
        std::shared_ptr<_Datatype> _semaphore;
    };

    DECLARE_SHARED_PTR(HashJoinInputAdapter);

    class _HashJoin : public _ProducingOperator
    {
    public:
        _HashJoin(std::shared_ptr<_Pipeline> leftPipeline, std::shared_ptr<_Pipeline> rightPipeline)
        {
            _leftSemaphore = POD("Semaphore", "leftWindowClosedSemaphore");
            _rightSemaphore = POD("Semaphore", "rightWindowClosedSemaphore");

            _leftPipeline = leftPipeline | HashJoinInputAdapter(_leftSemaphore);
            _rightPipeline = rightPipeline | HashJoinInputAdapter(_rightSemaphore);

            _keyType = std::static_pointer_cast<_MapType>(_leftPipeline->getPipelineOutputSchema()->getOnly())->getKeyType()->clone();
            _leftElementType =
                std::static_pointer_cast<_MapType>(_leftPipeline->getPipelineOutputSchema()->getOnly())->getValueType()->clone();
            _rightElementType =
                std::static_pointer_cast<_MapType>(_rightPipeline->getPipelineOutputSchema()->getOnly())->getValueType()->clone();
        };

        std::set<std::string> getHeaders() override;
        std::string getCode() override;
        std::string getGlobalDeclarations() override;
        std::shared_ptr<_Schema> getOutputSchema() override;
        using BaseType = _ProducingOperator;

    private:
        std::shared_ptr<_Pipeline> _leftPipeline, _rightPipeline;
        std::shared_ptr<_Datatype> _keyType, _leftElementType, _rightElementType, _leftSemaphore, _rightSemaphore;
    };

    DECLARE_SHARED_PTR(HashJoin);
}
