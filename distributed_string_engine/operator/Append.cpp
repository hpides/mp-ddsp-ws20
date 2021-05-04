#include "Append.h"

using namespace Operator;

std::string _Append::getCode() {
    std::stringstream stream;
    stream << _newDatatype->declaration() << std::endl
           << _newDatatype->id() << " = " << _initializationValue << ";" << std::endl
           << "{yield}";

    return stream.str();
}

std::shared_ptr<_Schema> _Append::getOutputSchema() {
    auto outputVariables = _inputSchema->getVariables();
    outputVariables.push_back(_newDatatype);

    return Schema(outputVariables);
}

std::set<std::string> _Append::getHeaders() {
    return {};
}

std::string _AppendTimestamp::getCode() {
    std::stringstream stream;
    stream << _timestamp->declaration() << std::endl
           << _timestamp->id() << " = getCurrentUnixTimestampInMilliseconds();" << std::endl
           << "{yield}";

    return stream.str();
}

std::shared_ptr<_Schema> _AppendTimestamp::getOutputSchema() {
    auto outputVariables = _inputSchema->getVariables();
    outputVariables.push_back(_timestamp);

    return Schema(outputVariables);
}

std::set<std::string> _AppendTimestamp::getHeaders() {
    return {"\"../../generated_code_libraries/Timestamp.h\""};
}
