#pragma once

#include <chrono>
#include <string>

#include "../Definitions.h"
#include "../Schema.h"
#include "Operator.h"

namespace Operator
{

    class _ProcessingTimeSlidingWindow : public _TransformingOperator
    {
    public:
        using BaseType = _TransformingOperator;

        explicit _ProcessingTimeSlidingWindow(long slideSizeMilliseconds, int slidesPerWindow)
            : _slideSizeMilliseconds(slideSizeMilliseconds)
            , _slidesPerWindow(slidesPerWindow)
            , _nextWindowClosesAtIdentifier("nextWindowClosesAt_" + _Datatype::generateUniqueVariableSuffix()){};

        std::string getCode() override;
        void setInputSchema(std::shared_ptr<_Schema> inputSchema) override;
        std::shared_ptr<_Schema> getOutputSchema() override;
        std::set<std::string> getHeaders() override;
        std::string getGlobalDeclarations() override;

    private:
        std::shared_ptr<_Schema> _outputSchema;
        long _slideSizeMilliseconds;
        int _slidesPerWindow;


        std::string _nextWindowClosesAtIdentifier;
    };

    DECLARE_SHARED_PTR(ProcessingTimeSlidingWindow);
}
