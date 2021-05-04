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
        _HashJoinInputAdapter(std::shared_ptr<_Datatype> closedSemaphore, std::shared_ptr<_Datatype> consumedSemaphore)
            : _closedSemaphore(closedSemaphore), _consumedSemaphore(consumedSemaphore)
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
        std::shared_ptr<_Datatype> _closedSemaphore, _consumedSemaphore;
    };

    DECLARE_SHARED_PTR(HashJoinInputAdapter);

    class _HashJoin : public _ProducingOperator
    {
    public:
        _HashJoin(std::shared_ptr<_Pipeline> leftPipeline, std::shared_ptr<_Pipeline> rightPipeline)
        {
            _leftWindowClosedSemaphore = POD("Semaphore", "leftWindowClosedSemaphore");
            _rightWindowClosedSemaphore = POD("Semaphore", "rightWindowClosedSemaphore");
            _leftWindowConsumedSemaphore = POD("Semaphore", "leftWindowConsumedSemaphore");
            _rightWindowConsumedSemaphore = POD("Semaphore", "rightWindowConsumedSemaphore");

            _leftPipeline = leftPipeline | HashJoinInputAdapter(_leftWindowClosedSemaphore, _leftWindowConsumedSemaphore);
            _rightPipeline = rightPipeline | HashJoinInputAdapter(_rightWindowClosedSemaphore, _rightWindowConsumedSemaphore);

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
        std::shared_ptr<_Datatype> _keyType, _leftElementType, _rightElementType, _leftWindowClosedSemaphore, _rightWindowClosedSemaphore, _leftWindowConsumedSemaphore, _rightWindowConsumedSemaphore;
    };

    DECLARE_SHARED_PTR(HashJoin);
}
