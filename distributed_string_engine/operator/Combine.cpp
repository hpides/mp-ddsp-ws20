#include "Combine.h"

using namespace Operator;

std::string _Combine::getCode() {
    std::stringstream stream;
    stream << _newDatatype->declaration() << std::endl
           << _newDatatype->id() << " = "
           << _aggregationStrategy->run(_inputSchema->getAccessor(_leftDatatype), _inputSchema->getAccessor(_rightDatatype)) << ";"
           << std::endl
           << "{yield}";

    return stream.str();
}

std::shared_ptr<_Schema> _Combine::getOutputSchema() {
    auto outputVariables = _inputSchema->getVariables();
    outputVariables.push_back(_newDatatype);

    return Schema(outputVariables);
}

std::set<std::string> _Combine::getHeaders() {
    return _aggregationStrategy->getHeaders();
}
