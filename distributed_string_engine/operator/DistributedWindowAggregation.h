#pragma once

#include <string>

#include "../Definitions.h"
#include "../Schema.h"
#include "Operator.h"

namespace Operator
{

    class _DistributedWindowAggregation : public _PipelineBreaker
    {
    public:
        explicit _DistributedWindowAggregation(long slideSizeMilliseconds, int slidesPerWindow, std::shared_ptr<_AggregationSchema> aggregationSchema)
            : _aggregationSchema(aggregationSchema)
            , _slideSizeMilliseconds(slideSizeMilliseconds)
            , _slidesPerWindow(slidesPerWindow)
            , _nextWindowClosesAt(POD("std::chrono::time_point<std::chrono::high_resolution_clock>", "nextWindowClosesAt"))
            , _windowClosedSemaphore(POD("Semaphore", "windowClosedSemaphore"))
            , _windowConsumedSemaphore(POD("Semaphore", "windowConsumedSemaphore"))
            , _mpiComm(POD("MPI_Comm", "comm")) {}

        std::set<std::string> getHeaders() override;
        std::string getGlobalDeclarations() override;
        std::string getCode() override;
        std::string getNewThreadCode() override;

        void setInputSchema(std::shared_ptr<_Schema> inputSchema) override;
        std::shared_ptr<_Schema> getOutputSchema() override;

        using BaseType = _TransformingOperator;

    private:
        std::shared_ptr<_Schema> _outputSchema;
        std::shared_ptr<_AggregationSchema> _aggregationSchema;
        std::shared_ptr<_Datatype> _tuplesPerRank;
        std::shared_ptr<_Datatype> _preSendAggregateMap;
        std::shared_ptr<_Datatype> _windowClosedSemaphore;
        std::shared_ptr<_Datatype> _windowConsumedSemaphore;
        std::shared_ptr<_Datatype> _reShuffledSlideContainers;
        std::shared_ptr<_Datatype> _rankSpecificWindowedAggregateMap;
        std::shared_ptr<_Datatype> _keyType;
        std::shared_ptr<_Datatype> _sendBuffer;
        std::shared_ptr<_Datatype> _mpiComm;
        size_t _keyIndex;

        long _slideSizeMilliseconds;
        int _slidesPerWindow;
        std::shared_ptr<_Datatype> _nextWindowClosesAt;

        static std::string getValueByIndex(uint index, const std::string& tupleName = "tuple");
        static std::string getElementAggregation(uint index, _AggregationSchema::AggregationType type, const std::string& tupleName = "tuple");
    };

    DECLARE_SHARED_PTR(DistributedWindowAggregation);
}
