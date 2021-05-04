#pragma once

#include <string>

#include "../Definitions.h"
#include "../Schema.h"
#include "Operator.h"

namespace Operator
{
    class _TCPReceiver : public _ProducingOperator
    {
    public:
        explicit _TCPReceiver(int port)
            : _port(port) {
            _socketDescriptor = POD("int", "socketDescriptor");
            _inputSocketFuture = POD("auto", "inputSocketFuture");
            _bufferSize = 4096 * 8 * 8 * 8;
        };

        std::string getGlobalDeclarations() override;
        std::string getCode() override;
        std::shared_ptr<_Schema> getOutputSchema() override;
        std::set<std::string> getHeaders() override;

        using BaseType = _ProducingOperator;

    private:
        int _port;
        size_t _bufferSize;
        std::shared_ptr<_Datatype> _inputSocketFuture;
        std::shared_ptr<_Datatype> _socketDescriptor;
    };

    DECLARE_SHARED_PTR(TCPReceiver);
}
