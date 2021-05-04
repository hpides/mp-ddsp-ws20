#include "Aggregation.h"

using namespace Operator;

void _Aggregation::setInputSchema(std::shared_ptr<_Schema> inputSchema) {

    bool keyFound = false;
    std::shared_ptr<_Datatype> keyType = POD("", "");
    for (uint i = 0; i < _aggregationSchema->getAggregationVariables().size(); i++) {
        const auto& [dataType, aggregationType] = _aggregationSchema->getAggregationVariables()[i];
        if (aggregationType == _AggregationSchema::key) {
            keyFound = true;
            keyType = dataType;
            _keyIndex = i;
            break;
        }
    }
    if (!keyFound) {
        throw std::runtime_error("No key provided for aggregation!");
    }

    _inputSchema = inputSchema;

    _queueType = _inputSchema->getOnly();
    auto queue = std::static_pointer_cast<_QueueType>(_inputSchema->getOnly());
    auto container = std::static_pointer_cast<_ContainerType>(queue->getValueType());

    _outputSchema = Schema(MapType(keyType, container->getValueType()));
}

std::shared_ptr<_Schema> _Aggregation::getOutputSchema() {
    return _outputSchema;
};

std::string _Aggregation::getCode() {
    std::stringstream stream;

    std::string aggregateMapName = _outputSchema->getOnly()->getVariableIdentifier();

    stream << _outputSchema->getOnly()->getDeclaration() << std::endl;
    stream << "for (int i = 0; i < " << _queueType->getVariableIdentifier() << ".size(); i++) {{" << std::endl;
    stream << "for (auto& tuple : " << _queueType->getVariableIdentifier() << "[i]) {{" << std::endl;
    stream << "     if (auto it = " << aggregateMapName << ".find(" << getValueByIndex(_keyIndex) << "); it == " << aggregateMapName
           << ".end()) {{" << std::endl;
    stream << "         " << aggregateMapName << "[" << getValueByIndex(_keyIndex) << "] = tuple;" << std::endl;
    stream << "     }} else {{" << std::endl;
    stream << "         auto& result = it->second;" << std::endl;
    for (uint i = 0; i < _aggregationSchema->getAggregationVariables().size(); i++) {
        auto const& [_, aggregationType] = _aggregationSchema->getAggregationVariables()[i];
        stream << getElementAggregation(i, aggregationType);
    }
    stream << "     }}" << std::endl;
    stream << "}}" << std::endl;
    stream << "}}" << std::endl;
    stream << "{yield}" << std::endl;
    return stream.str();
}

std::string _Aggregation::getValueByIndex(uint index, std::string tupleName) {
    return "std::get<" + std::to_string(index) + ">(" + tupleName + ")";
}

std::string _Aggregation::getElementAggregation(uint index, _AggregationSchema::AggregationType type) {
    std::stringstream stream;
    switch (type) {
    case _AggregationSchema::max:
        stream << getValueByIndex(index, "result") << " = std::max(" << getValueByIndex(index) << ", " << getValueByIndex(index, "result")
               << ");\n";
        break;
    case _AggregationSchema::min:
        stream << getValueByIndex(index, "result") << " = std::min(" << getValueByIndex(index) << ", " << getValueByIndex(index, "result")
               << ");\n";
    case _AggregationSchema::sum:
        stream << getValueByIndex(index, "result") << "+= " << getValueByIndex(index) << ";\n";
        break;
    case _AggregationSchema::key:
    case _AggregationSchema::noop:
        break;
    }

    return stream.str();
}
std::set<std::string> _Aggregation::getHeaders() {
    return {"<unordered_map>"};  // TODO: Actually belongs to Datatype not to Operator
}
